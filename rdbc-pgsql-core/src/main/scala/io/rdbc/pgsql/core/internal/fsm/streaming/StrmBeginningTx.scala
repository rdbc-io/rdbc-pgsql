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

import io.rdbc.pgsql.core._
import io.rdbc.pgsql.core.internal.fsm.{State, StateAction}
import io.rdbc.pgsql.core.internal.{PgMsgHandler, PgRowPublisher, PortalDescData}
import io.rdbc.pgsql.core.pgstruct.messages.backend.{CommandComplete, ReadyForQuery}
import io.rdbc.pgsql.core.pgstruct.messages.frontend._

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

private[core]
class StrmBeginningTx private[fsm](maybeParse: Option[Parse],
                                   bind: Bind,
                                   publisher: PgRowPublisher,
                                   describePromise: Promise[PortalDescData],
                                   parsePromise: Promise[Unit])
                                  (implicit out: ChannelWriter, ec: ExecutionContext)
  extends State {

  @volatile private[this] var beginComplete = false

  protected val msgHandler: PgMsgHandler = {
    case CommandComplete("BEGIN", _) =>
      beginComplete = true
      stay

    case ReadyForQuery(_) if beginComplete =>
      maybeParse.foreach(out.write(_))
      goto(
        State.Streaming.waitingForDescribe(
          txMgmt = true,
          publisher = publisher,
          portalName = bind.portal,
          describePromise = describePromise,
          parsePromise = parsePromise)
      ) andThen {
        out.writeAndFlush(bind, DescribePortal(bind.portal), Sync)
          .recoverWith { case NonFatal(ex) =>
            sendFailureToClient(ex)
            Future.failed(ex)
          }
      }
  }

  private def sendFailureToClient(ex: Throwable): Unit = {
    describePromise.failure(ex)
    parsePromise.failure(ex)
  }

  protected def onNonFatalError(ex: Throwable): StateAction = {
    goto(State.Streaming.queryFailed(txMgmt = true, bind.portal) {
      sendFailureToClient(ex)
    })
  }

  protected def onFatalError(ex: Throwable): Unit = {
    sendFailureToClient(ex)
  }

}
