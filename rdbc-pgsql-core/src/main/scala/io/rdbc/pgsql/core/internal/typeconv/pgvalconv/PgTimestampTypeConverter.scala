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

import io.rdbc.pgsql.core.internal.typeconv.InstantTypeConverter
import io.rdbc.pgsql.core.internal.typeconv.extractors.LocalDateTimeVal
import io.rdbc.pgsql.core.typeconv.PartialTypeConverter
import io.rdbc.pgsql.core.types.PgTimestamp

private[typeconv] object PgTimestampTypeConverter
  extends PartialTypeConverter[PgTimestamp] {

  val cls = classOf[PgTimestamp]

  def convert(any: Any): Option[PgTimestamp] = {
    any match {
      case LocalDateTimeVal(ldt) => Some(localDateTime2Timestamp(ldt))
      case _ => InstantTypeConverter.convert(any).map(PgTimestamp)
    }
  }

  private def localDateTime2Timestamp(ldt: LocalDateTime): PgTimestamp = {
    //TODO should this even be allowed?
    //SQL standard defines timestamp as local date time but PG stores it as instant
    PgTimestamp(ldt.toInstant(ZoneOffset.UTC))
    //TODO should the timezone setting be used here?
  }
}
