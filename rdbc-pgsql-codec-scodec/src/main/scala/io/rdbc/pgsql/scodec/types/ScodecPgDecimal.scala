/*
 * Copyright 2016-2017 Krzysztof Pado
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
import scodec.Attempt.Failure
import scodec.bits.BitVector
import scodec.codecs._
import scodec._

private[types] object PgDecimalCodec extends Codec[SqlNumeric] {

  private sealed trait PgDecSign
  private object PgDecSign {
    case object Negative extends PgDecSign
    case object Positive extends PgDecSign
    case object NaN extends PgDecSign
  }

  private case class DecimalParts(integer: String, fraction: String)
  private case class Header(digitsCount: Int, firstDigitWeight: Int, sign: PgDecSign, scale: Int)

  private[this] val signCodec: Codec[PgDecSign] = {
    discriminated
      .by("decimal_sign_id" | uint16)
      .typecase(0x4000, provide(PgDecSign.Negative).upcast[PgDecSign])
      .typecase(0x0000, provide(PgDecSign.Positive).upcast[PgDecSign])
      .typecase(0xC000, provide(PgDecSign.NaN).upcast[PgDecSign])
  }

  private[this] val headerCodec: Codec[Header] = {
    {
      ("digitsCount"      | uint16) ::
      ("firstDigitWeight" | uint16) ::
      ("sign"             | signCodec) ::
      ("scale"            | uint16)
    }.as[Header]
  }

  private[this] val NaNBits = headerCodec.encode {
    Header(digitsCount = 0, firstDigitWeight = 0, sign = PgDecSign.NaN, scale = 0)
  }

  private[this] val DigitLength = 4

  val sizeBound: SizeBound = headerCodec.sizeBound.atLeast

  def decode(bits: BitVector): Attempt[DecodeResult[SqlNumeric]] = {
    headerCodec.decode(bits).flatMap { headerResult =>
      val header = headerResult.value
      vectorOfN(
        provide(header.digitsCount),
        "digit" | int16
      ).decode(headerResult.remainder).map { digitsResult =>
        header.sign match {
          case PgDecSign.NaN => DecodeResult(SqlNumeric.NaN, digitsResult.remainder)
          case _ => decodeNonNan(header, digitsResult)
        }
      }
    }
  }

  private def decodeNonNan(header: Header,
                           digitsResult: DecodeResult[Vector[Int]]): DecodeResult[SqlNumeric] = {
    val dp = decimalPartsFromDigits(digitsResult.value, header.firstDigitWeight)

    val bigDecStr = {
      val signStr = if (header.sign == PgDecSign.Negative) "-" else ""
      val integerTrimmed = dropLeadingZeros(dp.integer)
      val fractionPadded = dp.fraction.padTo(header.scale, '0')
      signStr + integerTrimmed + (if (!fractionPadded.isEmpty) "." + fractionPadded else "")
    }

    DecodeResult(SqlNumeric.Val(BigDecimal(bigDecStr)), digitsResult.remainder)
  }

  private def decimalPartsFromDigits(digits: Vector[Int], firstDigitWeight: Int): DecimalParts = {
    val weight1 = firstDigitWeight + 1

    val digitsPadded = digits.padTo(weight1, 0)
    val digitStrs = digitsPadded.map("%04d".format(_))

    if (digitStrs.size > weight1) {
      DecimalParts(
        integer = digitStrs.slice(0, weight1).mkString,
        fraction = digitStrs.slice(weight1, digitStrs.size).mkString
      )
    } else DecimalParts(integer = digitStrs.mkString, fraction = "")
  }

  def encode(value: SqlNumeric): Attempt[BitVector] = {
    value match {
      case SqlNumeric.Val(bigDec) => encodeBigDec(bigDec)
      case SqlNumeric.NaN => NaNBits
      case SqlNumeric.NegInfinity | SqlNumeric.PosInfinity =>
        Failure(Err("Cannot encode infinity as a PostgreSQL decimal"))
    }
  }

  private def encodeBigDec(bigDec: BigDecimal): Attempt[BitVector] = {
    val dp = bigDecToDecimalParts(bigDec)
    val mergedNoDot = padInteger(dp.integer + dp.fraction)

    encode(
      sign = if (bigDec >= 0) PgDecSign.Positive else PgDecSign.Negative,
      weight = dp.integer.length / DigitLength,
      scale = bigDec.scale,
      digits = mergedNoDot.grouped(mergedNoDot.length / DigitLength).map(_.toInt).toVector
    )
  }

  private def bigDecToDecimalParts(bigDecimal: BigDecimal): DecimalParts = {
    val bigDecStr = bigDecimal.bigDecimal.toPlainString.toList.dropWhile(_ == '-')
    val (integer, fractionWithDot) = bigDecStr.span(_ != '.')

    DecimalParts(
      integer = padInteger(integer.mkString),
      fraction = padFraction(fractionWithDot.drop(1).mkString) //drop(1) is to drop a dot
    )
  }

  private def padInteger(num: String): String = {
    val digitCount = math.ceil(num.length.toDouble / DigitLength).toInt
    val padNeeded = (digitCount * DigitLength) - num.length
    ("0" * padNeeded) + num
  }

  private def padFraction(fraction: String): String = {
    val zeroTrimmed = dropTrailingZeros(fraction)
    val digitCount = math.ceil(zeroTrimmed.length.toDouble / DigitLength).toInt
    zeroTrimmed.padTo(digitCount * DigitLength, '0')
  }

  private def encode(sign: PgDecSign, weight: Int, scale: Int, digits: Vector[Int]): Attempt[BitVector] = {
    (headerCodec ~ vectorOfN(provide(digits.size), "digit" | int16)).encode(
      (
        Header(digits.size, weight, sign, scale),
        digits
      )
    )
  }

  private def dropTrailingZeros(str: String): String = {
    str.replaceAll("0+$", "")
  }

  private def dropLeadingZeros(str: String): String = {
    str.replaceAll("^0+", "")
  }
}

object ScodecPgDecimal extends ScodecPgType[SqlNumeric] with PgDecimal with CommonCodec[SqlNumeric] {
  def codec(implicit sessionParams: SessionParams): Codec[SqlNumeric] = PgDecimalCodec
}
