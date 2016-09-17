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

package io.rdbc.pgsql.codec.scodecimpl

import io.rdbc.pgsql.codec.scodecimpl.msg.backend._
import io.rdbc.pgsql.codec.scodecimpl.suppl._
import io.rdbc.pgsql.codec.{Decoded, Decoder, DecodingError}
import io.rdbc.pgsql.core.messages.backend.{Header, PgBackendMessage, ServerCharset}
import scodec.Attempt.{Failure, Successful}
import scodec.DecodeResult
import scodec.bits.ByteVector

class ScodecDecoder extends Decoder {
  override def decodeMsg(bytes: ByteVector)(implicit serverCharset: ServerCharset): Either[DecodingError, Decoded[PgBackendMessage]] = {
    pgBackendMessage.decode(bytes.bits) match {
      case Successful(DecodeResult(msg, remainder)) => Right(Decoded(msg, remainder.bytes))
      case Failure(err) => Left(DecodingError(err.messageWithContext))
    }
  }

  override def decodeHeader(bytes: ByteVector): Either[DecodingError, Decoded[Header]] = {
    header.decode(bytes.bits) match {
      case Successful(DecodeResult(msg, remainder)) => Right(Decoded(msg, remainder.bytes))
      case Failure(err) => Left(DecodingError(err.messageWithContext))
    }
    //TODO code dupl
  }
}
