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

package io.rdbc.pgsql.core.types

import io.rdbc.pgsql.core.Oid
import io.rdbc.sapi.DecimalNumber

case object PgNumericType extends PgType[PgNumeric] {
  val oid = Oid(1700)
  val valCls = classOf[PgNumeric]
  val name = "numeric"
}

final case class PgNumeric(value: DecimalNumber) extends PgVal[DecimalNumber] {
  val typ = PgNumericType
}
