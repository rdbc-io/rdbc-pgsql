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

package io.rdbc.pgsql.core.internal.typeconv

import io.rdbc.sapi.DecimalNumber

import scala.util.Try

object ExactNumberConversions {

  private[typeconv] implicit class ShortExactCast(val s: Short) extends AnyVal {
    def toByteExact: Option[Byte] = {
      val b = s.toByte
      if (b == s) Some(b)
      else None
    }
  }

  private[typeconv] implicit class IntExactCast(val i: Int) extends AnyVal {
    def toByteExact: Option[Byte] = {
      val b = i.toByte
      if (b == i) Some(b)
      else None
    }

    def toShortExact: Option[Short] = {
      val s = i.toShort
      if (s == i) Some(s)
      else None
    }
  }

  private[typeconv] implicit class LongExactCast(val l: Long) extends AnyVal {
    def toByteExact: Option[Byte] = {
      val b = l.toByte
      if (b == l) Some(b)
      else None
    }

    def toShortExact: Option[Short] = {
      val s = l.toShort
      if (s == l) Some(s)
      else None
    }

    def toIntExact: Option[Int] = {
      val s = l.toInt
      if (s == l) Some(s)
      else None
    }
  }

  private[typeconv] implicit class DoubleExactCast(val d: Double) extends AnyVal {

    def toFloatExact: Option[Float] = {
      val f = d.toFloat
      if (f == d) Some(f)
      else None
    }
  }

  private[typeconv] implicit class DecimalNumberExactCast(val n: DecimalNumber)
    extends AnyVal {

    def toByteExact: Option[Byte] = {
      n match {
        case DecimalNumber.Val(bd) => Try(bd.toByteExact).toOption
        case _ => None
      }
    }

    def toShortExact: Option[Short] = {
      n match {
        case DecimalNumber.Val(bd) => Try(bd.toShortExact).toOption
        case _ => None
      }
    }

    def toIntExact: Option[Int] = {
      n match {
        case DecimalNumber.Val(bd) => Try(bd.toIntExact).toOption
        case _ => None
      }
    }

    def toLongExact: Option[Long] = {
      n match {
        case DecimalNumber.Val(bd) => Try(bd.toLongExact).toOption
        case _ => None
      }
    }

    def toFloatExact: Option[Float] = {
      n match {
        case DecimalNumber.Val(bd) =>
          if (bd.isExactFloat) {
            Some(bd.toFloat)
          } else {
            None
          }

        case DecimalNumber.NaN => Some(Float.NaN)
        case DecimalNumber.PosInfinity => Some(Float.PositiveInfinity)
        case DecimalNumber.NegInfinity => Some(Float.NegativeInfinity)
      }
    }

    def toDoubleExact: Option[Double] = {
      n match {
        case DecimalNumber.Val(bd) =>
          if (bd.isExactDouble) {
            Some(bd.toDouble)
          } else {
            None
          }

        case DecimalNumber.NaN => Some(Float.NaN)
        case DecimalNumber.PosInfinity => Some(Float.PositiveInfinity)
        case DecimalNumber.NegInfinity => Some(Float.NegativeInfinity)
      }
    }
  }

}
