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

import java.time.{LocalDateTime, ZoneOffset}

import io.rdbc.pgsql.core.internal.typeconv.ZonedDateTimeTypeConverter
import io.rdbc.pgsql.core.internal.typeconv.extractors.LocalDateTimeVal
import io.rdbc.pgsql.core.typeconv.PartialTypeConverter
import io.rdbc.pgsql.core.types.PgTimestampTz

private[typeconv] object PgTimestampTzTypeConverter
  extends PartialTypeConverter[PgTimestampTz] {

  val cls = classOf[PgTimestampTz]

  def convert(any: Any): Option[PgTimestampTz] = {
    any match {
      case LocalDateTimeVal(ldt) => Some(localDateTime2TimestampTz(ldt))
      case _ => ZonedDateTimeTypeConverter.convert(any).map(PgTimestampTz)
    }
  }

  private def localDateTime2TimestampTz(ldt: LocalDateTime): PgTimestampTz = {
    //TODO should this even be allowed?
    //SQL standard defines timestamp as local date time but PG stores it as instant
    PgTimestampTz(ldt.atZone(ZoneOffset.UTC))
    //TODO should the timezone setting be used here?
  }
}
