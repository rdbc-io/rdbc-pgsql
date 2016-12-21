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
import io.rdbc.pgsql.core.pgstruct.ColValue

private[scodec] object ColValueCodec extends Codec[ColValue] {

  private[this] val nullLength = -1

  val sizeBound = SizeBound.exact(32) | SizeBound.atLeast(32)

  def decode(bits: BitVector): Attempt[DecodeResult[ColValue]] = {
    int32.withContext("col_val_length") //TODO maybe use "conditional" codec, check in other places if it can be used
      .decode(bits)
      .flatMap(lenResult => {
        val len = lenResult.value
        if (len == nullLength) {
          Attempt.successful(DecodeResult(ColValue.Null, lenResult.remainder))
        } else {
          bytes(len).withContext("col_val_bytes")
            .as[ColValue.NotNull]
            .decode(lenResult.remainder)
        }
      })
  }

  def encode(value: ColValue): Attempt[BitVector] = value match {
    case ColValue.Null => int32.unit(nullLength).encode(Unit)
    case ColValue.NotNull(data) => variableSizeBytes(int32, bytes).encode(data)
  }
}
