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
import io.rdbc.sapi.SqlSmallInt


private[typeconv] object ShortVal {
  def unapply(arg: Any): Option[Short] = {
    arg match {
      case s: Short => Some(s)
      case PgInt2(s) => Some(s)
      case SqlSmallInt(s) => Some(s)
      case ss: japi.SqlSmallInt => Some(ss.getValue)
      case _ => None
    }
  }
}
