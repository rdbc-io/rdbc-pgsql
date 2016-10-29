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

package io.rdbc.pgsql.core.auth

import com.typesafe.config.Config
import io.rdbc.pgsql.core.auth.AuthState.AuthComplete
import io.rdbc.pgsql.core.messages.backend.auth.{AuthBackendMessage, AuthRequestMd5}
import io.rdbc.pgsql.core.messages.frontend.PasswordMessage

import scala.collection.immutable


class UsernamePasswordAuthenticator(val username: String, val password: String) extends Authenticator {

  def this(config: Config) = {
    this(config.getString("username"), config.getString("password"))
  }

  override def authenticate(authReqMessage: AuthBackendMessage): AuthState = authReqMessage match {
    case req: AuthRequestMd5 =>
      AuthComplete(immutable.Seq(PasswordMessage.md5(username, password, req.salt)))
    //TODO more username password mechanisms
  }

  override def supports(authReqMessage: AuthBackendMessage): Boolean = authReqMessage match {
    case _: AuthRequestMd5 => true
    case _ => false
  }
}
