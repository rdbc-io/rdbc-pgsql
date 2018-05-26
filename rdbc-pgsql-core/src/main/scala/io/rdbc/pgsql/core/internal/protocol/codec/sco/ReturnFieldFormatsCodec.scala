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

import io.rdbc.pgsql.core.internal.protocol.ReturnColFormats
import scodec.bits.BitVector
import scodec.codecs.{int16, vectorOfN}
import scodec.{Attempt, Codec, Err, SizeBound}

private[sco] object ReturnFieldFormatsCodec extends Codec[ReturnColFormats] {
  def sizeBound: SizeBound = SizeBound.atLeast(1)

  def encode(value: ReturnColFormats): Attempt[BitVector] = value match {
    case ReturnColFormats.None | ReturnColFormats.AllTextual => int16.encode(0)
    case ReturnColFormats.AllBinary => (int16 ~ int16).encode((1, 1))
    case ReturnColFormats.Specific(formats) => vectorOfN(int16, colValFormat).encode(formats)
  }

  def decode(bits: BitVector): Attempt[Nothing] = {
    Attempt.failure(Err("decoding not supported"))
  }
}
