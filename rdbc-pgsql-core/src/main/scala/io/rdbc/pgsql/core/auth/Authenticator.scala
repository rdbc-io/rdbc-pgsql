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

import io.rdbc.ImmutSeq
import io.rdbc.pgsql.core.internal.protocol.messages.backend.auth.AuthBackendMessage
import io.rdbc.pgsql.core.internal.protocol.messages.frontend.PgFrontendMessage

trait Authenticator {
  def username: String
  def authenticate(authReqMessage: AuthBackendMessage): AuthState
  def supports(authReqMessage: AuthBackendMessage): Boolean
}

sealed trait AuthState {
  def responses: ImmutSeq[PgFrontendMessage]
}

object AuthState {
  final case class AuthContinue(responses: ImmutSeq[PgFrontendMessage]) extends AuthState
  final case class AuthComplete(responses: ImmutSeq[PgFrontendMessage]) extends AuthState
}
