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

package io.rdbc.pgsql.core.internal.protocol.codec.sco

import java.nio.charset.Charset

import io.rdbc.pgsql.core.exception.PgEncodeException
import io.rdbc.pgsql.core.internal.protocol.codec.MessageEncoder
import io.rdbc.pgsql.core.internal.protocol.codec.sco.frontend.pgFrontendMessage
import io.rdbc.pgsql.core.internal.protocol.messages.frontend.PgFrontendMessage
import scodec.Attempt.{Failure, Successful}
import scodec.bits.ByteVector

import scala.util.{Success, Try}

private[sco] class ScodecMessageEncoder(protected val charset: Charset) extends MessageEncoder {
  private[this] val codec = pgFrontendMessage(charset)

  def encode(msg: PgFrontendMessage): Try[ByteVector] = {
    codec.encode(msg) match {
      case Successful(bits) => Success(bits.bytes)
      case Failure(err) => util.Failure(new PgEncodeException(
        s"Error occurred while encoding message '$msg': ${err.messageWithContext}"
      ))
    }
  }
}
