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
import io.rdbc.pgsql.core.types._
import io.rdbc.sapi.{SqlChar, SqlClob, SqlNChar, SqlNClob, SqlNVarchar, SqlVarchar}

private[typeconv] object StringVal {
  def unapply(arg: Any): Option[String] = {
    arg match {
      case s: String => Some(s)
      case SqlVarchar(s) => Some(s)
      case sv: japi.SqlVarchar => Some(sv.getValue)
      case SqlChar(s) => Some(s)
      case sc: japi.SqlChar => Some(sc.getValue)
      case SqlNVarchar(s) => Some(s)
      case snv: japi.SqlNVarchar => Some(snv.getValue)
      case SqlNChar(s) => Some(s)
      case snc: japi.SqlNChar => Some(snc.getValue)
      case SqlClob(s) => Some(s)
      case sc: japi.SqlClob => Some(sc.getValue)
      case SqlNClob(s) => Some(s)
      case snc: japi.SqlNClob => Some(snc.getValue)
      case PgChar(s) => Some(s)
      case PgText(s) => Some(s)
      case PgVarchar(s) => Some(s)
      case _ => None
    }
  }
}
