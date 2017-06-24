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

package io.rdbc.pgsql.scodec

import java.nio.charset.Charset

import _root_.scodec.Attempt.{Failure, Successful}
import _root_.scodec.DecodeResult
import _root_.scodec.bits.ByteVector
import io.rdbc.pgsql.core.codec.{Decoded, Decoder}
import io.rdbc.pgsql.core.exception.PgDecodeException
import io.rdbc.pgsql.core.pgstruct.messages.backend.{MsgHeader, PgBackendMessage}
import io.rdbc.pgsql.scodec.msg.backend._

class ScodecDecoder(protected val charset: Charset) extends Decoder {

  private[this] val codec = pgBackendMessage(charset)

  def decodeMsg(bytes: ByteVector): Decoded[PgBackendMessage] = {
    decode(codec, bytes)
  }

  def decodeHeader(bytes: ByteVector): Decoded[MsgHeader] = {
    decode(header, bytes)
  }

  private def decode[A](decoder: scodec.Decoder[A], bytes: ByteVector): Decoded[A] = {
    decoder.decode(bytes.bits) match { //TODO data copying is a major bottleneck. decide
      case Successful(DecodeResult(msg, remainder)) => Decoded(msg, remainder.bytes)
      case Failure(err) => throw new PgDecodeException(
        s"Error occurred while decoding message ${bytes.toHex}: ${err.messageWithContext}"
      )
    }
  }
}
