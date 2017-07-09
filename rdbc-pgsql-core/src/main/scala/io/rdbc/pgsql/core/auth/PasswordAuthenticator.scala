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

package io.rdbc.pgsql.core.auth

import io.rdbc.pgsql.core.auth.AuthState.AuthComplete
import io.rdbc.pgsql.core.pgstruct.messages.backend.auth._
import io.rdbc.pgsql.core.pgstruct.messages.frontend.PasswordMessage

import scala.collection.immutable

class PasswordAuthenticator private[auth](val username: String, val password: String)
  extends Authenticator {

  def authenticate(authReqMessage: AuthBackendMessage): AuthState = authReqMessage match {
    case req: AuthRequestMd5 =>
      AuthComplete(immutable.Seq(PasswordMessage.md5(username, password, req.salt)))
    case AuthRequestCleartext =>
      AuthComplete(immutable.Seq(PasswordMessage.cleartext(password)))
  }

  def supports(authReqMessage: AuthBackendMessage): Boolean = {
    authReqMessage match {
      case _: AuthRequestMd5 | AuthRequestCleartext => true
      case _ => false
    }
  }

  override val toString = s"PasswordAuthenticator(username=$username, password=***)"
}
