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
import io.rdbc.pgsql.core.ChannelWriter
import io.rdbc.pgsql.core.fsm.State.Outcome
import io.rdbc.pgsql.core.fsm.{Idle, State, WaitingForReady}
import io.rdbc.pgsql.core.messages.backend.ReadyForQuery
import io.rdbc.pgsql.core.messages.frontend.Query

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object Failed {
  def apply(txMgmt: Boolean)(onIdle: => Unit)(implicit out: ChannelWriter, ec: ExecutionContext): Failed = {
    new Failed(txMgmt, onIdle)
  }
}

class Failed protected(txMgmt: Boolean, sendFailureCause: => Unit)(implicit out: ChannelWriter, ec: ExecutionContext)
  extends State
    with StrictLogging {

  def msgHandler = {
    case ReadyForQuery(txStatus) =>
      if (txMgmt) {
        goto(new WaitingForRollbackCompletion(sendFailureCause)) andThen {
          out.writeAndFlush(Query("ROLLBACK")).recoverWith {
            case NonFatal(ex) =>
              sendFailureToClient(ex)
              Future.failed(ex)
          }
        }
      } else {
        goto(Idle(txStatus)) andThenF sendFailureCause
      }
  }

  def sendFailureToClient(ex: Throwable): Unit = {
    logger.error("Error occurred when handling failed operation", ex)
    sendFailureCause
  }

  protected def onNonFatalError(ex: Throwable): Outcome = {
    goto(new WaitingForReady(onIdle = sendFailureToClient(ex), onFailure = { exWhenWaiting =>
      logger.error("Error occurred when waiting for ready", exWhenWaiting)
      sendFailureToClient(ex)
    })) //TODO this pattern repeats
  }

  protected def onFatalError(ex: Throwable): Unit = {
    sendFailureToClient(ex)
  }

  val name = "extended_querying.failed"
}
