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
import io.rdbc.pgsql.core.internal.protocol.messages.backend.ReadyForQuery

private[core]
class WaitingForReady private[fsm](onIdle: => Unit, onFailure: Throwable => Unit)
  extends State
    with NonFatalErrorsAreFatal {

  protected val msgHandler: PgMsgHandler = {
    case ReadyForQuery(txStatus) => goto(State.idle(txStatus)) andThenF onIdle
  }

  protected def onError(ex: Throwable): Unit = traced {
    onFailure(ex)
  }
}
