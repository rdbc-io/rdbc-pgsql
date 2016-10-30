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

package io.rdbc.pgsql.scodec

import java.nio.charset.Charset

import io.rdbc.pgsql.core.codec.Encoder
import io.rdbc.pgsql.core.messages.frontend._
import io.rdbc.pgsql.scodec.msg.frontend._

class ScodecEncoder extends Encoder {
  override def encode(msg: PgFrontendMessage)(implicit charset: Charset): Array[Byte] = {
    val codec = msg match {
      case m: StartupMessage => startup.upcast[PgFrontendMessage]
      case m: Bind => bind.upcast[PgFrontendMessage]
      case m: Describe => describe.upcast[PgFrontendMessage]
      case m: Execute => execute.upcast[PgFrontendMessage]
      case m: Parse => parse.upcast[PgFrontendMessage]
      case m: PasswordMessage => password.upcast[PgFrontendMessage]
      case m: Query => query.upcast[PgFrontendMessage]
      case m: CancelRequest => cancelRequest.upcast[PgFrontendMessage]
      case m: CloseStatement => closeStatement.upcast[PgFrontendMessage]
      case m: ClosePortal => closePortal.upcast[PgFrontendMessage]
      case Terminate => terminate.upcast[PgFrontendMessage]
      case Flush => flush.upcast[PgFrontendMessage]
      case Sync => sync.upcast[PgFrontendMessage]
    }

    codec.encode(msg).require.toByteArray
  }
}
