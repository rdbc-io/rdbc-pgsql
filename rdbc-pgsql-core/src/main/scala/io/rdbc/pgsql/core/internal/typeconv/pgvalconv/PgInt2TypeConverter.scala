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

package io.rdbc.pgsql.core.internal.typeconv.pgvalconv

import io.rdbc.pgsql.core.internal.typeconv.ShortTypeConverter
import io.rdbc.pgsql.core.typeconv.PartialTypeConverter
import io.rdbc.pgsql.core.types.PgInt2

private[typeconv] object PgInt2TypeConverter
  extends PartialTypeConverter[PgInt2] {

  val cls = classOf[PgInt2]

  def convert(any: Any): Option[PgInt2] = {
    ShortTypeConverter.convert(any).map(PgInt2)
  }
}
