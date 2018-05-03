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

import io.rdbc.pgsql.core.internal.typeconv.extractors._
import io.rdbc.pgsql.core.typeconv.PartialTypeConverter
import io.rdbc.sapi.DecimalNumber

private[typeconv] object DecimalNumberTypeConverter
  extends PartialTypeConverter[DecimalNumber] {
  val cls = classOf[DecimalNumber]

  def convert(any: Any): Option[DecimalNumber] = {
    any match {
      case DecimalNumberVal(dn) => Some(dn)
      case ByteVal(b) => Some(DecimalNumber.Val(BigDecimal(b.toInt)))
      case ShortVal(s) => Some(DecimalNumber.Val(BigDecimal(s.toInt)))
      case IntVal(i) => Some(DecimalNumber.Val(i))
      case LongVal(l) => Some(DecimalNumber.Val(l))
      case FloatVal(Float.NaN) => Some(DecimalNumber.NaN)
      case FloatVal(Float.PositiveInfinity) => Some(DecimalNumber.PosInfinity)
      case FloatVal(Float.NegativeInfinity) => Some(DecimalNumber.NegInfinity)
      case FloatVal(f) => Some(DecimalNumber.Val(BigDecimal(f.toDouble)))
      case DoubleVal(Double.NaN) => Some(DecimalNumber.NaN)
      case DoubleVal(Double.PositiveInfinity) => Some(DecimalNumber.PosInfinity)
      case DoubleVal(Double.NegativeInfinity) => Some(DecimalNumber.NegInfinity)
      case DoubleVal(d) => Some(DecimalNumber.Val(BigDecimal(d)))
      case _ => None
    }
  }
}
