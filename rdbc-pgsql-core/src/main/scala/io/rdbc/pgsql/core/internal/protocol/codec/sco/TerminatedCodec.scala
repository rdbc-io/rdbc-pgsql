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

import scodec._
import scodec.bits.BitVector

private[sco] class TerminatedCodec[A](terminator: BitVector, codec: Codec[A]) extends Codec[A] {

  def sizeBound: SizeBound = SizeBound.unknown

  def decode(bits: BitVector): Attempt[DecodeResult[A]] = {
    bits.bytes.indexOfSlice(terminator.bytes) match {
      case -1 => Attempt.failure(Err(s"does not contain a '${terminator.toHex}' terminator"))
      case i => codec.decode(bits.take(i * 8L)).map(dr => dr.mapRemainder(_ => bits.drop(i * 8L + 8L)))
    }
  }

  def encode(value: A): Attempt[BitVector] = codec.encode(value).map(_ ++ terminator)
}
