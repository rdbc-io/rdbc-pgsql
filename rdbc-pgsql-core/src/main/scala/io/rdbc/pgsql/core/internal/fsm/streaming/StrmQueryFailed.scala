/*
 * Copyright 2016-2017 Krzysztof Pado
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rdbc.pgsql.core.internal.fsm.streaming

import com.typesafe.scalalogging.StrictLogging
import io.rdbc.pgsql.core.internal.fsm.{Idle, State, StateAction}
import io.rdbc.pgsql.core.pgstruct.messages.backend.{CloseComplete, ReadyForQuery}
import io.rdbc.pgsql.core.pgstruct.messages.frontend._
import io.rdbc.pgsql.core.{ChannelWriter, PgMsgHandler}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

private[core]
class StrmQueryFailed private[fsm](txMgmt: Boolean,
                                   portalName: Option[PortalName],
                                   sendFailureCause: => Unit)
                                  (implicit out: ChannelWriter,
                                   ec: ExecutionContext)
  extends State
    with StrictLogging {

  @volatile private[this] var portalClosed = false

  protected val msgHandler: PgMsgHandler = {
    case CloseComplete if !portalClosed =>
      portalClosed = true
      stay

    case ReadyForQuery(txStatus) =>
      if (txMgmt) {
        rollback()
      } else {
        if (!portalClosed) {
          closePortal()
        } else {
          goto(Idle(txStatus)) andThenF sendFailureCause
        }
      }
  }

  private def rollback(): StateAction.Goto = {
    goto(State.Streaming.waitingAfterRollback(sendFailureCause)) andThen {
      out.writeAndFlush(Query(NativeSql("ROLLBACK"))).recoverWith { //TODO interpolator nsql?
        case NonFatal(ex) =>
          sendFailureToClient(ex)
          Future.failed(ex)
      }
    }
  }

  private def closePortal(): StateAction.Stay = {
    stay andThen out.writeAndFlush(ClosePortal(portalName), Sync).recoverWith {
      case NonFatal(ex) =>
        sendFailureToClient(ex)
        Future.failed(ex)
    }
  }

  private def sendFailureToClient(ex: Throwable): Unit = {
    logger.error("Error occurred when handling failed operation", ex)
    sendFailureCause
  }

  protected def onNonFatalError(ex: Throwable): StateAction = {
    goto(State.waitingAfterFailure(sendFailureToClient(_), ex))
  }

  protected def onFatalError(ex: Throwable): Unit = {
    sendFailureToClient(ex)
  }
}
