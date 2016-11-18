/*
 * Copyright 2016 Krzysztof Pado
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

package io.rdbc.pgsql.core.fsm.extendedquery

import com.typesafe.scalalogging.StrictLogging
import io.rdbc.pgsql.core.fsm.State.Outcome
import io.rdbc.pgsql.core.fsm._
import io.rdbc.pgsql.core.messages.backend.CommandComplete

class WaitingForRollbackCompletion(sendFailureCause: => Unit)
  extends State
    with StrictLogging {

  def msgHandler = {
    case CommandComplete("ROLLBACK", _) =>
      goto(new WaitingForReady(
        onIdle = sendFailureCause,
        onFailure = { ex =>
          logger.error("Error occurred when waiting for ready", ex)
          sendFailureCause
        }
      ))
  }

  def sendFailureToClient(ex: Throwable): Unit = {
    logger.error("Error occurred when handling failed operation", ex)
    sendFailureCause
  }

  protected def onNonFatalError(ex: Throwable): Outcome = {
    goto(new WaitingForReady(
      onIdle = sendFailureToClient(ex),
      onFailure = { ex =>
        logger.error("Error occurred when waiting for ready", ex)
        sendFailureToClient(ex)
      }))
  }

  protected def onFatalError(ex: Throwable): Unit = {
    sendFailureToClient(ex)
  }

  val name = "extended_querying.waiting_for_ready_after_commit"

}