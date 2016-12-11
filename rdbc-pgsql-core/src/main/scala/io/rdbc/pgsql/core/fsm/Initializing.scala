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
import io.rdbc.pgsql.core.exception.ProtocolViolationException
import io.rdbc.pgsql.core.messages.backend.{BackendKeyData, ReadyForQuery}

import scala.concurrent.{ExecutionContext, Promise}

class Initializing(initPromise: Promise[BackendKeyData])(implicit out: ChannelWriter, ec: ExecutionContext)
  extends State with NonFatalErrorsAreFatal {

  private var backendKeyData: Option[BackendKeyData] = None

  def msgHandler = {
    case bkd: BackendKeyData =>
      backendKeyData = Some(bkd)
      stay

    case ReadyForQuery(txStatus) =>
      backendKeyData match {
        case Some(bkd) => goto(Idle(txStatus)) andThenF initPromise.success(bkd)
        case None =>
          val ex = new ProtocolViolationException("Ready for query received in initializing state without prior backend key data message")
          fatal(ex) andThenF initPromise.failure(ex)
      }
  }

  protected def onFatalError(ex: Throwable): Unit = {
    initPromise.failure(ex)
  }

  val name = "initializing"
}
