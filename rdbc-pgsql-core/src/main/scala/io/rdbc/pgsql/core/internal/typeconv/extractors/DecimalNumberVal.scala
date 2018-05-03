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

package io.rdbc.pgsql.core.internal.typeconv.extractors

import io.rdbc.japi
import io.rdbc.pgsql.core.internal.typeconv.DecimalNumberToScala
import io.rdbc.pgsql.core.types._
import io.rdbc.sapi.{DecimalNumber, SqlDecimal, SqlNumeric}

private[typeconv] object DecimalNumberVal {
  def unapply(arg: Any): Option[DecimalNumber] = {
    arg match {
      case dn: DecimalNumber => Some(dn)
      case dn: japi.DecimalNumber => Some(dn.toScala)
      case PgNumeric(dn) => Some(dn)
      case SqlNumeric(dn) => Some(dn)
      case sn: japi.SqlNumeric => Some(sn.getValue.toScala)
      case SqlDecimal(dn) => Some(dn)
      case sd: japi.SqlDecimal => Some(sd.getValue.toScala)
      case bd: BigDecimal => Some(DecimalNumber.Val(bd))
      case bd: java.math.BigDecimal => Some(DecimalNumber.Val(bd))
      case _ => None
    }
  }
}
