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

package io.rdbc.pgsql.transport

import java.net.InetSocketAddress
import java.nio.charset.Charset

import io.rdbc.pgsql.core.messages.backend.PgBackendMessage
import io.rdbc.pgsql.core.messages.frontend.PgFrontendMessage

object ConnectionManager {

  val DefaultCharset = Charset.forName("US-ASCII")

  /* receiving */
  case class Connect(address: InetSocketAddress)

  case object Close

  case class Write(msgs: PgFrontendMessage*)

  /* sending */
  case class Received(msg: PgBackendMessage)

  case object ConnectFailed

  case object ConnectionClosed

  case object WriteFailed

  case object Connected

  case class SyncError(msg: String)

}
