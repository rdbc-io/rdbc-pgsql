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

private[typeconv] object FloatTypeConverter
  extends PartialTypeConverter[Float] {

  val cls = classOf[Float]

  def convert(any: Any): Option[Float] = {
    any match {
      case FloatVal(f) => Some(f)
      case DoubleVal(d) => d.toFloatExact
      case DecimalNumberVal(dn) => dn.toFloatExact
      case _ => None
    }
  }

}
