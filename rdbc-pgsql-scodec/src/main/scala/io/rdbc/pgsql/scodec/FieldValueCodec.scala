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

import _root_.scodec.bits.{BitVector, ByteVector}
import _root_.scodec.codecs._
import _root_.scodec.{Attempt, Codec, DecodeResult, SizeBound}
import io.rdbc.pgsql.core.messages.data.{FieldValue, NotNullFieldValue, NullFieldValue}

object FieldValueCodec extends Codec[FieldValue] {
  def sizeBound: SizeBound = SizeBound.unknown

  def decode(bits: BitVector): Attempt[DecodeResult[FieldValue]] = {
    pgInt32.decode(bits).flatMap(dResult => {
      val len = dResult.value
      if (len == -1) {
        Attempt.successful(DecodeResult(NullFieldValue, dResult.remainder))
      } else {
        bytesArr(len).as[NotNullFieldValue].decode(dResult.remainder)
      }
    })
  }

  def encode(value: FieldValue): Attempt[BitVector] = value match {
    case NullFieldValue => byte.unit(-1).encode(Unit)
    case NotNullFieldValue(data) => variableSizeBytes(pgInt32, bytes).encode(ByteVector.view(data))
  }
}
