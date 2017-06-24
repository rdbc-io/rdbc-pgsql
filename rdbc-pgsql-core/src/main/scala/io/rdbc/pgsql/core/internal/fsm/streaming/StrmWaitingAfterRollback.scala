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

import io.rdbc.pgsql.core.PgMsgHandler
import io.rdbc.pgsql.core.internal.fsm._
import io.rdbc.pgsql.core.pgstruct.messages.backend.CommandComplete
import io.rdbc.util.Logging

private[core]
class StrmWaitingAfterRollback private[fsm](sendFailureCause: => Unit)
  extends State
    with Logging {

  protected val msgHandler: PgMsgHandler = {
    case CommandComplete("ROLLBACK", _) =>
      goto(
        new WaitingForReady(
          onIdle = sendFailureCause,
          onFailure = { ex =>
            logger.error("Error occurred when waiting for ready after the rollback", ex)
            sendFailureCause
          }
        ))
  }

  def sendFailureToClient(ex: Throwable): Unit = {
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
