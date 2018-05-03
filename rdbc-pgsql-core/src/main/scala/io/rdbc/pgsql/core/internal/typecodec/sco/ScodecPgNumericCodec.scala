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

package io.rdbc.pgsql.core.internal.typecodec.sco

import io.rdbc.pgsql.core.types.{PgNumeric, PgNumericType}
import io.rdbc.sapi.DecimalNumber
import scodec.Attempt.Failure
import scodec.bits.BitVector
import scodec.codecs._
import scodec.{Attempt, Codec, DecodeResult, Err, SizeBound}

private[sco] object DecimalNumberCodec extends Codec[DecimalNumber] {

  private sealed trait Sign

  private object Sign {

    case object Negative extends Sign

    case object Positive extends Sign

    case object NaN extends Sign

  }

  private case class DecimalParts(integer: String, fraction: String)

  private case class Header(digitsCount: Int,
                            firstDigitWeight: Int,
                            sign: Sign,
                            scale: Int)

  private[this] val signCodec: Codec[Sign] = {
    discriminated[Sign]
      .by("decimal_sign_id" | uint16)
      .typecase(0x4000, provide(Sign.Negative))
      .typecase(0x0000, provide(Sign.Positive))
      .typecase(0xC000, provide(Sign.NaN))
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
    Header(digitsCount = 0, firstDigitWeight = 0, sign = Sign.NaN, scale = 0)
  }

  private[this] val DigitLength = 4

  val sizeBound: SizeBound = headerCodec.sizeBound.atLeast

  def decode(bits: BitVector): Attempt[DecodeResult[DecimalNumber]] = {
    headerCodec.decode(bits).flatMap { headerResult =>
      val header = headerResult.value
      vectorOfN(
        provide(header.digitsCount),
        "digit" | int16
      ).decode(headerResult.remainder).map { digitsResult =>
        header.sign match {
          case Sign.NaN => DecodeResult(DecimalNumber.NaN, digitsResult.remainder)
          case _ => decodeNonNan(header, digitsResult)
        }
      }
    }
  }

  private def decodeNonNan(header: Header,
                           digitsResult: DecodeResult[Vector[Int]]): DecodeResult[DecimalNumber] = {
    val dp = decimalPartsFromDigits(digitsResult.value, header.firstDigitWeight)

    val bigDecStr = {
      val signStr = if (header.sign == Sign.Negative) "-" else ""
      val integerTrimmed = dropLeadingZeros(dp.integer)
      val fractionPadded = dp.fraction.padTo(header.scale, '0')
      signStr + integerTrimmed + (if (!fractionPadded.isEmpty) "." + fractionPadded else "")
    }

    DecodeResult(DecimalNumber.Val(BigDecimal(bigDecStr)), digitsResult.remainder)
  }

  private def decimalPartsFromDigits(digits: Vector[Int], firstDigitWeight: Int): DecimalParts = {
    val weight1 = {
      if (firstDigitWeight == 65535) 0
      else firstDigitWeight + 1
    }

    val digitsPadded = digits.padTo(weight1, 0)
    val digitStrs = digitsPadded.map("%04d".format(_))

    if (digitStrs.size > weight1) {
      DecimalParts(
        integer = digitStrs.slice(0, weight1).mkString,
        fraction = digitStrs.slice(weight1, digitStrs.size).mkString
      )
    } else DecimalParts(integer = digitStrs.mkString, fraction = "")
  }

  def encode(value: DecimalNumber): Attempt[BitVector] = {
    value match {
      case DecimalNumber.Val(bigDec) => encodeBigDec(bigDec)
      case DecimalNumber.NaN => NaNBits
      case DecimalNumber.NegInfinity | DecimalNumber.PosInfinity =>
        Failure(Err("Cannot encode infinity as a PostgreSQL decimal"))
    }
  }

  private def encodeBigDec(bigDec: BigDecimal): Attempt[BitVector] = {
    val dp = bigDecToDecimalParts(bigDec)
    val mergedNoDot = padInteger(dp.integer + dp.fraction)

    encode(
      sign = if (bigDec >= 0) Sign.Positive else Sign.Negative,
      weight = (dp.integer.length / DigitLength) - 1,
      scale = bigDec.scale,
      digits = mergedNoDot.grouped(DigitLength).map(_.toInt).toVector
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

  private def encode(sign: Sign, weight: Int, scale: Int, digits: Vector[Int]): Attempt[BitVector] = {
    (headerCodec ~ vectorOfN(provide(digits.size), "digit" | int16)).encode(
      (
        Header(digits.size, weight, sign, scale),
        digits
      )
    )
  }

  private def dropTrailingZeros(str: String): String = {
    dropLeadingZeros(str.reverse).reverse
  }

  private def dropLeadingZeros(str: String): String = {
    val zerosDropped = str.dropWhile(_ == '0')
    if (zerosDropped.nonEmpty) zerosDropped
    else "0"
  }
}

private[typecodec] object ScodecPgNumericCodec
  extends ScodecPgValCodec[PgNumeric]
    with IgnoreSessionParams[PgNumeric] {

  val typ = PgNumericType
  val codec = DecimalNumberCodec.as[PgNumeric]
}
