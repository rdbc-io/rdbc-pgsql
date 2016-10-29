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
import io.rdbc.pgsql.core.SessionParams
import io.rdbc.pgsql.core.types.Timestamp
import io.rdbc.pgsql.scodec._

object ScodecTimestamp extends Timestamp with ScodecPgType[LocalDateTime] with CommonCodec[LocalDateTime] {
  def codec(implicit sessionParams: SessionParams): Codec[LocalDateTime] = {
    pgInt64.xmap(
      long2LocalDateTime,
      localDateTime2Long
    )
  }

  private val pgZero: ZonedDateTime = LocalDate.of(2000, Month.JANUARY, 1).atStartOfDay(ZoneId.of("UTC"))

  private def long2LocalDateTime(l: Long): LocalDateTime = {
    pgZero.plus(l, MICROS).toLocalDateTime
  }

  private def localDateTime2Long(ldt: LocalDateTime): Long = {
    val zdt = ZonedDateTime.of(ldt, ZoneId.of("UTC"))
    val dur = Duration.between(pgZero, zdt)
    val micros = (dur.getSeconds * 1000L * 1000L) + (dur.getNano / 1000L)
    micros
  }
}
