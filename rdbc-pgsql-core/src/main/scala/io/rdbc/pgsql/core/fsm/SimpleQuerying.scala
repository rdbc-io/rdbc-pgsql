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

package io.rdbc.pgsql.core.fsm

import io.rdbc.pgsql.core.ChannelWriter
import io.rdbc.pgsql.core.exception.PgStatusDataException
import io.rdbc.pgsql.core.fsm.State.Outcome
import io.rdbc.pgsql.core.messages.backend._

import scala.concurrent.Promise

sealed trait SimpleQuerying extends State {
  def subName: String
  val name = "simple_querying." + subName
}

object SimpleQuerying {
  class PullingRows(out: ChannelWriter, promise: Promise[Unit]) extends SimpleQuerying {

    def msgHandler = {
      case _: RowDescription => stay
      case _: DataRow => stay
      case CommandComplete(_, _) | EmptyQueryResponse => goto(new SuccessWaitingForReady(promise))
      case StatusMessage.Error(statusData) => goto(new FailureWaitingForReady(PgStatusDataException(statusData), promise))
    }

    override protected def onNonFatalError(ex: Throwable): Outcome = {
      goto(new FailureWaitingForReady(ex, promise))
    }

    protected def onFatalError(ex: Throwable): Unit = {
      promise.failure(ex)
    }

    val subName = "pulling_rows"
  }

  class SuccessWaitingForReady(promise: Promise[Unit]) extends SimpleQuerying
    with NonFatalErrorsAreFatal {

    def msgHandler = {
      case ReadyForQuery(txStatus) => goto(Idle(txStatus)) andThen promise.success(())
    }

    protected def onFatalError(ex: Throwable): Unit = {
      promise.failure(ex)
    }

    val subName = "success_waiting_for_ready"
  }

  class FailureWaitingForReady(ex: Throwable, promise: Promise[Unit]) extends SimpleQuerying
    with NonFatalErrorsAreFatal {

    def msgHandler = {
      case ReadyForQuery(txStatus) => goto(Idle(txStatus)) andThen promise.failure(ex)
    }

    protected def onFatalError(ex: Throwable): Unit = {
      promise.failure(ex)
    }

    val subName = "failure_waiting_for_ready"
  }
}
