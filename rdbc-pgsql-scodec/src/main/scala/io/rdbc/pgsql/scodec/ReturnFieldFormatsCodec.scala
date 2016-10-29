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

import _root_.scodec.bits.BitVector
import _root_.scodec.codecs._
import _root_.scodec.{Attempt, Codec, DecodeResult, SizeBound}
import io.rdbc.pgsql.core.messages.frontend._

class ReturnFieldFormatsCodec extends Codec[ReturnFieldFormats] {
  override def encode(value: ReturnFieldFormats): Attempt[BitVector] = value match {
    case NoReturnFields | AllTextual => pgInt16.encode(0)
    case AllBinary => (pgInt16 ~ pgInt16).encode(1, 1)
    case SpecificFieldFormats(formats) => listOfN(pgInt16, dbValFormat).encode(formats)
  }

  override def sizeBound: SizeBound = SizeBound.atLeast(1)

  override def decode(bits: BitVector): Attempt[DecodeResult[ReturnFieldFormats]] = ??? //TODO
}