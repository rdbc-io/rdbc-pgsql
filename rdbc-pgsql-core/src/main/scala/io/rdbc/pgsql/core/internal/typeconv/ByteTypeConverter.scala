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

private[typeconv] object ByteTypeConverter
  extends PartialTypeConverter[Byte] {

  val cls = classOf[Byte]

  def convert(any: Any): Option[Byte] = {
    any match {
      case ByteVal(b) => Some(b)
      case ShortVal(s) => s.toByteExact
      case IntVal(i) => i.toByteExact
      case LongVal(i) => i.toByteExact
      case DecimalNumberVal(dn) => dn.toByteExact
      case _ => None
    }
  }

}
