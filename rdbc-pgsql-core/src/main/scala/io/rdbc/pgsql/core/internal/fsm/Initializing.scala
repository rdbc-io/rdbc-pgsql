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

package io.rdbc.pgsql.core.internal.fsm

import io.rdbc.pgsql.core.PgMsgHandler
import io.rdbc.pgsql.core.exception.PgProtocolViolationException
import io.rdbc.pgsql.core.pgstruct.messages.backend.{BackendKeyData, ReadyForQuery}

import scala.concurrent.Promise

class Initializing(initPromise: Promise[BackendKeyData])
  extends State
    with NonFatalErrorsAreFatal {

  @volatile private[this] var maybeKeyData = Option.empty[BackendKeyData]

  protected val msgHandler: PgMsgHandler = {
    case bkd: BackendKeyData =>
      maybeKeyData = Some(bkd)
      stay

    case ReadyForQuery(txStatus) =>
      maybeKeyData.fold[StateAction] {
        val ex = new PgProtocolViolationException(
          "Ready for query received in initializing state without prior backend key data message"
        )
        fatal(ex) andThenF initPromise.failure(ex)
      }(bkd => goto(State.idle(txStatus)) andThenF initPromise.success(bkd))
  }

  protected def onError(ex: Throwable): Unit = traced {
    initPromise.failure(ex)
    ()
  }
}
