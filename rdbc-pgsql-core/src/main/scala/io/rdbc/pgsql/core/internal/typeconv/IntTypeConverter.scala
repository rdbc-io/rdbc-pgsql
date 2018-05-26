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

import io.rdbc.pgsql.core.internal.typeconv.ExactNumberConversions._
import io.rdbc.pgsql.core.internal.typeconv.extractors._
import io.rdbc.pgsql.core.typeconv.PartialTypeConverter

private[typeconv] object IntTypeConverter
  extends PartialTypeConverter[Int] {

  val cls = classOf[Int]

  def convert(any: Any): Option[Int] = {
    any match {
      case IntVal(i) => Some(i)
      case ByteVal(b) => Some(b.toInt)
      case ShortVal(s) => Some(s.toInt)
      case LongVal(l) => l.toIntExact
      case DecimalNumberVal(dn) => dn.toIntExact
      case _ => None
    }
  }
}
