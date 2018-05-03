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

import java.time._

import io.rdbc.japi
import io.rdbc.pgsql.core.types.PgTime
import io.rdbc.sapi.SqlTime

private[typeconv] object LocalTimeVal {
  def unapply(any: Any): Option[LocalTime] = {
    any match {
      case lt: LocalTime => Some(lt)
      case PgTime(lt) => Some(lt)
      case SqlTime(lt) => Some(lt)
      case st: japi.SqlTime => Some(st.getValue)
      case _ => None
    }
  }
}
