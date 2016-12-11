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

package io.rdbc.pgsql.core.fsm.extendedquery.batch

import io.rdbc.pgsql.core.fsm.State.Outcome
import io.rdbc.pgsql.core.fsm.{State, WaitingForReady}
import io.rdbc.pgsql.core.messages.backend._

import scala.concurrent.Promise

class ExecutingBatch(promise: Promise[TxStatus]) extends State {

  protected def msgHandler = {
    case ParseComplete => stay
    case BindComplete => stay
    case _: DataRow => stay
    case EmptyQueryResponse | _: CommandComplete => stay
    case ReadyForQuery(txStatus) =>
      promise.success(txStatus)
      stay
  }

  protected def onFatalError(ex: Throwable): Unit = promise.failure(ex)

  protected def onNonFatalError(ex: Throwable): Outcome = {
    goto(new WaitingForReady(onIdle = promise.failure(ex), onFailure = { exWhenWaiting =>
      logger.error("Error occurred when waiting for ready", exWhenWaiting)
      promise.failure(ex)
    })) //TODO this repeats throughout the project
  }

  val name = "executing_batch"
}
