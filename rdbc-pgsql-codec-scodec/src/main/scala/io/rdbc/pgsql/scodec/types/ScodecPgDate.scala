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
import java.time.temporal.ChronoUnit.DAYS

import _root_.scodec.Codec
import _root_.scodec.codecs.int32
import io.rdbc.pgsql.core.SessionParams
import io.rdbc.pgsql.core.types.PgDate

object ScodecPgDate extends ScodecPgType[LocalDate] with PgDate with CommonCodec[LocalDate] {
  def codec(implicit sessionParams: SessionParams): Codec[LocalDate] = {
    int32.xmap(
      int2LocalDate,
      localDate2Int
    )
  }

  private[this] val PgZero: LocalDate = LocalDate.of(2000, Month.JANUARY, 1)

  private def int2LocalDate(i: Int): LocalDate = {
    PgZero.plus(i, DAYS)
  }

  private def localDate2Int(ld: LocalDate): Int = {
    Period.between(PgZero, ld).getDays
  }
}
