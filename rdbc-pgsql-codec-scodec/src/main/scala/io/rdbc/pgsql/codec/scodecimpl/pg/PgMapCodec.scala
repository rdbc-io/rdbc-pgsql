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

package io.rdbc.pgsql.codec.scodecimpl.pg

import scodec.bits.BitVector
import scodec.codecs._
import scodec.{Attempt, Codec, DecodeResult, Encoder, Err, SizeBound}

import scala.collection.mutable.ListBuffer

class PgMapCodec[K](codec: Codec[(K, String)]) extends Codec[Map[K, String]] {

  private val nul = BitVector.lowByte

  def sizeBound = SizeBound.unknown

  def encode(options: Map[K, String]) = Encoder.encodeSeq(codec)(options.toList).map(_ ++ nul)

  def decode(buffer: BitVector): Attempt[DecodeResult[Map[K, String]]] = {
    val builder = ListBuffer.empty[(K, String)]
    var remaining = buffer
    var count = 0
    var error: Option[Err] = None
    while (remaining.sizeGreaterThanOrEqual(8) && remaining.slice(0, 8) != nul) {
      codec.decode(remaining) match {
        case Attempt.Successful(DecodeResult(value, rest)) =>
          builder += value
          count += 1
          remaining = rest
        case Attempt.Failure(err) =>
          error = Some(err.pushContext(count.toString))
          remaining = BitVector.empty
      }
    }
    error match {
      case None => constant(nul).withContext("collection terminator").decode(remaining).map(dr => DecodeResult(Map(builder: _*), dr.remainder))
      case Some(err) => Attempt.failure(err)
    }
  }

  override def toString = s"pgMap($codec)"
}