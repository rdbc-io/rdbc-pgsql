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

import _root_.scodec.bits.BitVector
import _root_.scodec.codecs._
import _root_.scodec.{Attempt, Codec, DecodeResult, Encoder, Err, SizeBound}

import scala.collection.mutable.ListBuffer

class PgMapCodec[K](codec: Codec[(K, String)]) extends Codec[Map[K, String]] {

  private[this] val nul = BitVector.lowByte

  val sizeBound = SizeBound.unknown

  def encode(options: Map[K, String]): Attempt[BitVector] = {
    Encoder.encodeSeq(codec)(options.toList).map(_ ++ nul)
  }

  def decode(bits: BitVector): Attempt[DecodeResult[Map[K, String]]] = {
    val listBuffer = ListBuffer.empty[(K, String)]
    var remaining = bits
    var count = 0
    var error: Option[Err] = None
    /* iterate until map key is nul */
    while (remaining.sizeGreaterThanOrEqual(8) && remaining.slice(0, 8) != nul) {
      codec.decode(remaining) match {
        case Attempt.Successful(DecodeResult(value, rest)) =>
          listBuffer += value
          count += 1
          remaining = rest
        case Attempt.Failure(err) =>
          error = Some(err.pushContext(s"element-$count"))
          remaining = BitVector.empty
      }
    }
    error match {
      case None =>
        constant(nul).decode(remaining).map {
          dr => DecodeResult(Map(listBuffer: _*), dr.remainder)
        }

      case Some(err) => Attempt.failure(err)
    }
  }
}
