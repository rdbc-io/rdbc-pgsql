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

package io.rdbc.pgsql.netty.fsm

import io.rdbc.api.exceptions.RdbcException
import io.rdbc.pgsql.core.exception.PgStmtExecutionEx
import io.rdbc.pgsql.core.messages.backend._
import io.rdbc.pgsql.netty.ChannelWriter

import scala.concurrent.Promise

sealed trait SimpleQuerying extends State

object SimpleQuerying {
  class PullingRows(out: ChannelWriter, promise: Promise[Unit]) extends SimpleQuerying {

    def handleMsg = {
      case _: RowDescription => stay
      case _: DataRow => stay
      case CommandComplete(_, _) | EmptyQueryResponse => goto(new SuccessWaitingForReady(promise))
      case ErrorMessage(statusData) => goto(new FailedWaitingForReady(PgStmtExecutionEx(statusData), promise))
    }

    val shortDesc = "simple_querying.pulling_rows"
  }

  class SuccessWaitingForReady(promise: Promise[Unit]) extends SimpleQuerying {
    def handleMsg = {
      case ReadyForQuery(txStatus) => goto(Idle(txStatus)) andThen promise.success(())
    }

    val shortDesc = "simple_querying.waiting_for_ready"
  }

  class FailedWaitingForReady(rdbcEx: RdbcException, promise: Promise[Unit]) extends SimpleQuerying {
    def handleMsg = {
      case ReadyForQuery(txStatus) => goto(Idle(txStatus)) andThen promise.failure(rdbcEx)
    }

    val shortDesc = "simple_querying.waiting_for_ready"
  }
}
