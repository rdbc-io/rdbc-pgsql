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

import java.time._
import java.time.temporal.ChronoUnit.MICROS

import _root_.scodec.Codec
import io.rdbc.pgsql.core.SessionParams
import io.rdbc.pgsql.core.types.PgTime
import io.rdbc.pgsql.scodec._

object ScodecPgTime extends PgTime with ScodecPgType[LocalTime] with CommonCodec[LocalTime] {
  def codec(implicit sessionParams: SessionParams): Codec[LocalTime] = {
    pgInt64.xmap(
      long2LocalTime,
      localTime2Long
    )
  }

  private val PgZero: LocalTime = LocalTime.MIDNIGHT

  private def long2LocalTime(l: Long): LocalTime = {
    PgZero.plus(l, MICROS)
  }

  private def localTime2Long(ldt: LocalTime): Long = {
    val dur = Duration.between(PgZero, ldt)
    val micros = (dur.getSeconds * 1000L * 1000L) + (dur.getNano / 1000L) //TODO this repeats
    micros
  }
}
