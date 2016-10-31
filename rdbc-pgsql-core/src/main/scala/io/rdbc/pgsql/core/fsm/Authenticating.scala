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

import io.rdbc.api.exceptions.ConnectException.AuthFailureException
import io.rdbc.pgsql.core.ChannelWriter
import io.rdbc.pgsql.core.auth.AuthState.{AuthComplete, AuthContinue}
import io.rdbc.pgsql.core.auth.Authenticator
import io.rdbc.pgsql.core.exception.PgConnectException
import io.rdbc.pgsql.core.messages.backend.auth.{AuthOk, AuthRequest}
import io.rdbc.pgsql.core.messages.backend.{BackendKeyData, StatusMessage}

import scala.concurrent.{ExecutionContext, Promise}

class Authenticating(initPromise: Promise[BackendKeyData], authenticator: Authenticator)
                    (implicit out: ChannelWriter, ec: ExecutionContext) extends State {

  private var waitingForOk = false

  def handleMsg = {
    case authReq: AuthRequest if !waitingForOk =>
      if (authenticator.supports(authReq)) {
        val answersToWrite = authenticator.authenticate(authReq) match {
          case AuthContinue(answers) => answers
          case AuthComplete(answers) =>
            waitingForOk = true
            answers
        }
        out.writeAndFlush(answersToWrite)
        stay
      } else {
        val ex = AuthFailureException(s"Authentication mechanism '${authReq.authMechanismName}' requested by server is not supported by provided authenticator")
        fatal(ex) andThenFailPromise initPromise
      }

    case AuthOk if waitingForOk => goto(new Initializing(initPromise))

    case StatusMessage.Error(statusData) =>
      val ex = PgConnectException(statusData)
      fatal(ex) andThenFailPromise initPromise
  }

  val name = "authenticating"
}
