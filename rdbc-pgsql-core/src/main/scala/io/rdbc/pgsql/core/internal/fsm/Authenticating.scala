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

import io.rdbc.api.exceptions.AuthFailureException
import io.rdbc.pgsql.core.auth.AuthState.{AuthComplete, AuthContinue}
import io.rdbc.pgsql.core.auth.{AuthState, Authenticator}
import io.rdbc.pgsql.core.pgstruct.messages.backend.BackendKeyData
import io.rdbc.pgsql.core.pgstruct.messages.backend.auth.{AuthOk, AuthRequest}
import io.rdbc.pgsql.core.{ChannelWriter, PgMsgHandler}

import scala.concurrent.{ExecutionContext, Promise}

private[core]
class Authenticating private[fsm](initPromise: Promise[BackendKeyData],
                                  authenticator: Authenticator)
                                 (implicit out: ChannelWriter,
                                  ec: ExecutionContext)
  extends State
    with NonFatalErrorsAreFatal {

  private[this] var waitingForOk = false

  val msgHandler: PgMsgHandler = {
    case authReq: AuthRequest if !waitingForOk =>
      ifAuthenticatorSupports(authReq) {
        val authState = authenticator.authenticate(authReq)
        if (isComplete(authState)) {
          waitingForOk = true
        }
        stay andThenF out.writeAndFlush(authState.responses)
      }

    case AuthOk if waitingForOk => goto(new Initializing(initPromise))
  }

  private def ifAuthenticatorSupports(authReq: AuthRequest)(body: => StateAction): StateAction = {
    if (authenticator.supports(authReq)) {
      body
    } else {
      val ex = new AuthFailureException(
        s"Authentication mechanism '${authReq.authMechanismName}' requested by" +
          " server is not supported by provided authenticator"
      )
      fatal(ex) andThenFailPromise initPromise
    }
  }

  private def isComplete(authState: AuthState): Boolean = {
    authState match {
      case _: AuthContinue => false
      case _: AuthComplete => true
    }
  }

  protected def onError(ex: Throwable): Unit = {
    initPromise.failure(ex)
  }

}
