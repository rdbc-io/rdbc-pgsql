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

import io.rdbc.pgsql.core.exception.PgDecodeException
import io.rdbc.pgsql.core.internal.protocol.codec.sco.backend.pgBackendMessage
import io.rdbc.pgsql.core.internal.protocol.codec.{DecodedMessage, MessageDecoder}
import io.rdbc.pgsql.core.internal.protocol.messages.backend.{MsgHeader, PgBackendMessage}
import scodec.Attempt._
import scodec._
import scodec.bits.ByteVector

import scala.util.{Success, Try}

private[sco] class ScodecMessageDecoder(protected val charset: Charset) extends MessageDecoder {

  private[this] val codec = pgBackendMessage(charset)

  def decodeMsg(bytes: ByteVector): Try[DecodedMessage[PgBackendMessage]] = {
    decode(codec, bytes)
  }

  def decodeHeader(bytes: ByteVector): Try[DecodedMessage[MsgHeader]] = {
    decode(header, bytes)
  }

  private def decode[A](decoder: _root_.scodec.Decoder[A], bytes: ByteVector): Try[DecodedMessage[A]] = {
    decoder.decode(bytes.bits) match { //TODO data copying is a major bottleneck. decide
      case Successful(DecodeResult(msg, remainder)) => Success(DecodedMessage(msg, remainder.bytes))
      case Failure(err) => util.Failure(new PgDecodeException(
        s"Error occurred while decoding message ${bytes.toHex}: ${err.messageWithContext}"
      ))
    }
  }
}
