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

import io.rdbc.pgsql.core.messages.backend.{BackendKeyData, ReadyForQuery}
import io.rdbc.pgsql.netty.ChannelWriter
import io.rdbc.pgsql.netty.exception.ProtocolViolationException
import io.rdbc.sapi.Connection

import scala.concurrent.{ExecutionContext, Future, Promise}

class Initializing(out: ChannelWriter, initPromise: Promise[BackendKeyData])(implicit ec: ExecutionContext) extends State {

  private var backendKeyData: Option[BackendKeyData] = None

  def handleMsg = {
    case bkd: BackendKeyData =>
      backendKeyData = Some(bkd)
      stay

    case ReadyForQuery(txStatus) =>
      backendKeyData match {
        case Some(bkd) =>
          goto(Idle(txStatus)) andThen initPromise.success(bkd)

        case None =>
          initPromise.failure(new ProtocolViolationException("Ready for query received in initializing state without prior backend key data message"))
          stay //TODO fatal ex
      }
  }

  val shortDesc = "initializing"
}
