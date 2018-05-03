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

private[typeconv] object BoolTypeConverter
  extends PartialTypeConverter[Boolean] {

  val cls = classOf[Boolean]

  def convert(any: Any): Option[Boolean] = {
    any match {
      case BoolVal(b) => Some(b)
      case ByteVal(b) => num2Bool(b)
      case ShortVal(s) => num2Bool(s)
      case IntVal(i) => num2Bool(i)
      case LongVal(l) => num2Bool(l)
      case DecimalNumberVal(dn) => decimal2Bool(dn)
      case CharVal(c) => char2Bool(c)
      case StringVal(s) => str2Bool(s)
      case _ => None
    }
  }

  private def num2Bool(n: java.lang.Number): Option[Boolean] = {
    n.doubleValue() match {
      case 1.0 => Some(true)
      case 0.0 => Some(false)
      case _ => None
    }
  }

  private def decimal2Bool(dn: DecimalNumber): Option[Boolean] = {
    dn match {
      case DecimalNumber.Val(bd) => num2Bool(bd)
      case _ => None
    }
  }

  private def str2Bool(str: String): Option[Boolean] = {
    if (str.length == 1) {
      char2Bool(str.head)
    } else {
      None
    }
  }

  private def char2Bool(char: Char): Option[Boolean] = {
    if (char == '1' || char == 'T' || char == 'Y') Some(true)
    else if (char == '0' || char == 'F' || char == 'N') Some(false)
    else None
  }

}
