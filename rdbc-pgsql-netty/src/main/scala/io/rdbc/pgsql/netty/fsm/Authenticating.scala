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

import io.rdbc.pgsql.core.auth.AuthState.{AuthComplete, AuthContinue}
import io.rdbc.pgsql.core.auth.Authenticator
import io.rdbc.pgsql.core.exception.PgConnectEx
import io.rdbc.pgsql.core.messages.backend.auth.{AuthBackendMessage, AuthOk}
import io.rdbc.pgsql.core.messages.backend.{BackendKeyData, ErrorMessage}
import io.rdbc.pgsql.netty.ChannelWriter

import scala.concurrent.{ExecutionContext, Promise}

class Authenticating(out: ChannelWriter, initPromise: Promise[BackendKeyData],
                     authenticator: Authenticator)(implicit ec: ExecutionContext) extends State {

  private var waitingForOk = false

  def handleMsg = {
    case authMsg: AuthBackendMessage if !waitingForOk =>
      authenticator.authenticate(authMsg) match {
        case AuthContinue(answers) =>
          out.writeAndFlush(answers)
          stay

        case AuthComplete(answers) =>
          out.writeAndFlush(answers)
          waitingForOk = true
          stay
      }

    case AuthOk if waitingForOk => goto(new Initializing(out, initPromise))

    case ErrorMessage(statusData) =>
      initPromise.failure(PgConnectEx(statusData))
      stay //TODO fatal error
  }

  val shortDesc = "authenticating"
}
