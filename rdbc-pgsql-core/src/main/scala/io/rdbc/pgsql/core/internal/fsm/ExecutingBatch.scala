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

package io.rdbc.pgsql.core.internal.fsm

import io.rdbc.pgsql.core.internal.PgMsgHandler
import io.rdbc.pgsql.core.pgstruct.TxStatus
import io.rdbc.pgsql.core.pgstruct.messages.backend._

import scala.concurrent.Promise

private[core]
class ExecutingBatch private[fsm](batchPromise: Promise[TxStatus])
  extends State {

  protected val msgHandler: PgMsgHandler = {
    case ParseComplete => stay
    case BindComplete => stay
    case _: DataRow => stay
    case EmptyQueryResponse | _: CommandComplete => stay
    case ReadyForQuery(txStatus) =>
      goto(State.waitingForNextBatch).andThenF(batchPromise.success(txStatus))
  }

  protected def onFatalError(ex: Throwable): Unit = traced {
    batchPromise.failure(ex)
    ()
  }

  protected def onNonFatalError(ex: Throwable): StateAction = traced {
    goto(State.waitingAfterFailure(batchPromise, ex))
  }
}
