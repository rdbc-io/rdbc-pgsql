/*
 * Copyright 2016 rdbc contributors
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

import io.rdbc.pgsql.core.ChannelWriter
import io.rdbc.pgsql.core.internal.fsm.{State, StateAction}
import io.rdbc.pgsql.core.internal.protocol.messages.backend.ReadyForQuery
import io.rdbc.pgsql.core.internal.protocol.messages.frontend.{ClosePortal, Sync}
import io.rdbc.pgsql.core.internal.{PgMsgHandler, PgRowPublisher}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

private[core]
class StrmPendingClosePortal private[fsm](publisher: PgRowPublisher, onIdle: => Unit)
                                         (implicit out: ChannelWriter, ec: ExecutionContext)
  extends State {

  val msgHandler: PgMsgHandler = {
    case ReadyForQuery(_) =>
      goto(new StrmWaitingAfterClose(onIdle, publisher)) andThen {
        out.writeAndFlush(ClosePortal(publisher.portalName), Sync)
          .recoverWith { case NonFatal(ex) =>
            sendFailureToClient(ex)
            Future.failed(ex)
          }
      }
  }

  private def sendFailureToClient(ex: Throwable): Unit = {
    publisher.failure(ex)
  }

  protected def onNonFatalError(ex: Throwable): StateAction = {
    goto(State.waitingAfterFailure(sendFailureToClient(_), ex))
  }

  protected def onFatalError(ex: Throwable): Unit = {
    sendFailureToClient(ex)
  }
}
