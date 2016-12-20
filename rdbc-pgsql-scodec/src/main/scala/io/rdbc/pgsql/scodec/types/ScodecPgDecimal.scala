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

package io.rdbc.pgsql.scodec.types

import io.rdbc.pgsql.core.SessionParams
import io.rdbc.pgsql.core.types.PgDecimal
import io.rdbc.sapi.SqlNumeric
import scodec.Attempt.{Failure, Successful}
import scodec.bits.BitVector
import scodec.codecs._
import scodec.{Codec, _}
import shapeless.{::, HNil}

object PgDecimalCodec extends Codec[SqlNumeric] {

  private sealed trait PgDecSign
  private object PgDecSign {
    case object Negative extends PgDecSign
    case object Positive extends PgDecSign
    case object NaN extends PgDecSign
  }

  private case class DecimalParts(integer: String, fraction: String)

  private val signCodec = uint16.exmap[PgDecSign](//TODO this can be done via discriminator or sth
    {
      case 0x4000 => Successful(PgDecSign.Negative)
      case 0x0000 => Successful(PgDecSign.Positive)
      case 0xC000 => Successful(PgDecSign.NaN)
      case unknownSignCode => Failure(Err(s"Value '$unknownSignCode' is not a valid decimal sign code"))
    }, {
      case PgDecSign.Negative => Successful(0x4000)
      case PgDecSign.Positive => Successful(0x0000)
      case PgDecSign.NaN => Successful(0xC000)
    }
  )

  private val headerCodec = {
    ("digitsCount" | uint16) ::
      ("firstDigitWeight" | uint16) ::
      ("sign" | signCodec) ::
      ("scale" | uint16)
  }

  private val NaNBits = headerCodec.encode(0 :: 0 :: PgDecSign.NaN :: 0 :: HNil)
  private val DigitLength = 4
  private val Int16Size = 2
  private val BitsInByte = 8

  val sizeBound = SizeBound.atLeast(Int16Size * DigitLength * BitsInByte)

  def decode(bits: BitVector): Attempt[DecodeResult[SqlNumeric]] = {
    headerCodec.decode(bits).flatMap { headerResult =>
      val (digitsCount :: firstDigitWeight :: sign :: scale :: HNil) = headerResult.value
      vectorOfN(provide(digitsCount), int16).decode(headerResult.remainder).map { digitsResult =>
        if (sign == PgDecSign.NaN) {
          DecodeResult(SqlNumeric.NaN, digitsResult.remainder)
        } else {
          val weight1 = firstDigitWeight + 1

          /* convert digits to 1000-base and apply a weight adjustment */
          val digits = digitsResult.value.padTo(weight1, 0)
          val digitStrs = digits.map(digit => "%04d".format(digit))

          val dp = if (digitStrs.size > weight1) {
            DecimalParts(
              integer = digitStrs.slice(0, weight1).mkString,
              fraction = digitStrs.slice(weight1, digitStrs.size).mkString
            )
          } else DecimalParts(integer = digitStrs.mkString, fraction = "")

          val bigDecStr = {
            val signStr = if (sign == PgDecSign.Negative) "-" else ""
            val integerTrimmed = dp.integer.dropWhile(_ == '0')
            val fractionPadded = dp.fraction.padTo(scale, '0')
            signStr + integerTrimmed + (if (!fractionPadded.isEmpty) "." + fractionPadded else "")
          }

          DecodeResult(SqlNumeric.Val(BigDecimal(bigDecStr)), digitsResult.remainder)
        }
      }
    }
  }

  def encode(value: SqlNumeric): Attempt[BitVector] = {
    value match {
      case SqlNumeric.NaN => NaNBits
      case SqlNumeric.NegInfinity | SqlNumeric.PosInfinity => Failure(Err("Cannot encode infinity as a PostgreSQL decimal"))
      case SqlNumeric.Val(bigDec) =>
        val dp = prepareDecimalParts(bigDec)
        val mergedNoDot = padInteger(dp.integer + dp.fraction)

        encode(
          sign = if (bigDec >= 0) PgDecSign.Positive else PgDecSign.Negative,
          weight = dp.integer.length / DigitLength,
          scale = bigDec.scale,
          digits = mergedNoDot.grouped(mergedNoDot.length / DigitLength).map(_.toInt).toVector
        )
    }
  }

  private def prepareDecimalParts(bigDecimal: BigDecimal): DecimalParts = {
    val bigDecStr = bigDecimal.bigDecimal.toPlainString.toList.dropWhile(_ == '-')
    val (integer, fraction) = bigDecStr.span(_ != '.')

    DecimalParts(
      integer = padInteger(integer.mkString),
      fraction = padFraction(fraction.drop(1).mkString) //drop is to drop a dot
    )
  }

  private def padInteger(num: String): String = {
    val digitCount = math.ceil(num.length.toDouble / DigitLength).toInt
    val padNeeded = (digitCount * DigitLength) - num.length
    ("0" * padNeeded) + num
  }

  private def padFraction(fraction: String): String = {
    val zeroTrimmed = fraction.reverse.dropWhile(_ == '0').reverse
    val digitCount = math.ceil(zeroTrimmed.length.toDouble / DigitLength).toInt
    zeroTrimmed.padTo(digitCount * DigitLength, '0')
  }

  private def encode(sign: PgDecSign, weight: Int, scale: Int, digits: Vector[Int]): Attempt[BitVector] = {
    (headerCodec ~ vectorOfN(provide(digits.size), int16)).encode(
      (
        digits.size :: weight :: sign :: scale :: HNil,
        digits
      )
    )
  }
}

object ScodecPgDecimal extends PgDecimal with ScodecPgType[SqlNumeric] with CommonCodec[SqlNumeric] {
  def codec(implicit sessionParams: SessionParams): Codec[SqlNumeric] = PgDecimalCodec
}
