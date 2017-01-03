/*
 * Copyright 2016 Krzysztof Pado
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

package io.rdbc.pgsql.scodec.types

import java.time.temporal.ChronoUnit.MICROS
import java.time.{ZonedDateTime, _}

import _root_.scodec.Codec
import _root_.scodec.codecs.int64
import io.rdbc.pgsql.core.SessionParams
import io.rdbc.pgsql.core.types.PgTimestamp

object ScodecPgTimestamp extends ScodecPgType[LocalDateTime] with PgTimestamp with CommonCodec[LocalDateTime] {
  def codec(implicit sessionParams: SessionParams): Codec[LocalDateTime] = {
    int64.xmap(
      long2LocalDateTime,
      localDateTime2Long
    )
  }

  private[this] val PgZero: ZonedDateTime = {
    LocalDate.of(2000, Month.JANUARY, 1).atStartOfDay(ZoneId.of("UTC"))
  }

  private def long2LocalDateTime(l: Long): LocalDateTime = {
    PgZero.plus(l, MICROS).toLocalDateTime
  }

  private def localDateTime2Long(ldt: LocalDateTime): Long = {
    val zdt = ZonedDateTime.of(ldt, ZoneId.of("UTC"))
    Duration.between(PgZero, zdt).toMicros
  }
}
