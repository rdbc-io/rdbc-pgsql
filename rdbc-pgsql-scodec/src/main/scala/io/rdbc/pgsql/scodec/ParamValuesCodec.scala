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

import java.nio.charset.Charset

import _root_.scodec.bits.{BitVector, ByteVector}
import _root_.scodec.codecs._
import _root_.scodec.{Attempt, Codec, DecodeResult, SizeBound}
import io.rdbc.pgsql.core.messages.frontend.{BinaryDbValue, DbValue, NullDbValue, TextualDbValue}

class ParamValuesCodec(implicit charset: Charset) extends Codec[List[DbValue]] {
  def sizeBound: SizeBound = SizeBound.unknown

  def encode(params: List[DbValue]): Attempt[BitVector] = {
    val lenBits = pgInt16.encode(params.length)

    val paramFormatAttempts = params.map {
      case _: NullDbValue | _: TextualDbValue => pgInt16.encode(0)
      case _: BinaryDbValue => pgInt16.encode(1)
    }
    val paramFormatBits = concatAttempts(paramFormatAttempts)

    val paramValueAttempts: List[Attempt[BitVector]] = params.map {
      case _: NullDbValue => pgInt32.encode(-1)
      case TextualDbValue(value, _) => variableSizeBytes(pgInt32, pgStringNonTerminated).encode(value) //TODO check whether not cstring
      case BinaryDbValue(value, _) => variableSizeBytes(pgInt32, bytes).encode(ByteVector.view(value))
    }

    val paramValueBits = concatAttempts(paramValueAttempts)

    concatAttempts(Traversable(lenBits, paramFormatBits, lenBits, paramValueBits))
  }

  def decode(bits: BitVector): Attempt[DecodeResult[List[DbValue]]] = ??? //TODO

  private def concatAttempts(attempts: Traversable[Attempt[BitVector]]): Attempt[BitVector] = {
    attempts.foldLeft(Attempt.successful(BitVector.empty)) { (accAttempt, attempt) =>
      accAttempt.flatMap(accBits => attempt.map(bits => accBits ++ bits))
    }
  }
}