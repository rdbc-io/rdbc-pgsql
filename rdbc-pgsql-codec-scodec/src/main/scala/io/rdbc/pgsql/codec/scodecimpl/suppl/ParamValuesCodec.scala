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

package io.rdbc.pgsql.codec.scodecimpl.suppl

import java.nio.charset.Charset

import io.rdbc.pgsql.codec.scodecimpl.pg._
import io.rdbc.pgsql.core.messages.frontend.{BinaryDbValue, NullDbValue, DbValue, TextualDbValue}
import scodec.bits.BitVector
import scodec.codecs._
import scodec.{Attempt, Codec, DecodeResult, SizeBound}

class ParamValuesCodec(implicit charset: Charset) extends Codec[List[DbValue]] {
  override def encode(params: List[DbValue]): Attempt[BitVector] = {
    val lenBits = pgInt16.encode(params.length)

    val paramFormatAttempts = params.map {
      case NullDbValue | TextualDbValue(_) => pgInt16.encode(0)
      case BinaryDbValue(_) => pgInt16.encode(1)
    }
    val paramFormatBits = concatAttempts(paramFormatAttempts)

    val paramValueAttempts: List[Attempt[BitVector]] = params.map {
      case NullDbValue => pgInt32.encode(-1)
      case TextualDbValue(value) => variableSizeBytes(pgInt32, pgStringNonTerminated).encode(value) //TODO check whether not cstring
      case BinaryDbValue(value) => variableSizeBytes(pgInt32, bytes).encode(value)
    }

    val paramValueBits = concatAttempts(paramValueAttempts)

    concatAttempts(Traversable(lenBits, paramFormatBits, lenBits, paramValueBits))
  }

  def concatAttempts(attempts: Traversable[Attempt[BitVector]]): Attempt[BitVector] = {
    attempts.foldLeft(Attempt.successful(BitVector.empty)) { (accAttempt, attempt) =>
      accAttempt.flatMap(accBits => attempt.map(bits => accBits ++ bits))
    }
  }

  override def decode(bits: BitVector): Attempt[DecodeResult[List[DbValue]]] = ??? //TODO

  override def sizeBound: SizeBound = SizeBound.unknown
}