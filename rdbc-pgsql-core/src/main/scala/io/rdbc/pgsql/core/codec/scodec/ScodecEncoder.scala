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

package io.rdbc.pgsql.core.codec.scodec

import java.nio.charset.Charset

import _root_.scodec.Attempt.{Failure, Successful}
import _root_.scodec.bits.ByteVector
import io.rdbc.pgsql.core.codec.Encoder
import io.rdbc.pgsql.core.internal.scodec.msg.frontend.pgFrontendMessage
import io.rdbc.pgsql.core.exception.PgEncodeException
import io.rdbc.pgsql.core.pgstruct.messages.frontend._


class ScodecEncoder(protected val charset: Charset) extends Encoder {
  private[this] val codec = pgFrontendMessage(charset)

  def encode(msg: PgFrontendMessage): ByteVector = {
    codec.encode(msg) match {
      case Successful(bits) => bits.bytes
      case Failure(err) => throw new PgEncodeException(
        s"Error occurred while encoding message '$msg': ${err.messageWithContext}"
      )
    }
  }
}
