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

package io.rdbc.pgsql.core.internal.typecodec.sco

import java.time._
import java.time.temporal.ChronoUnit.DAYS

import io.rdbc.pgsql.core.types.{PgDate, PgDateType}
import scodec.codecs.int32

private[typecodec] object ScodecPgDateCodec
  extends ScodecPgValCodec[PgDate]
    with IgnoreSessionParams[PgDate] {

  val typ = PgDateType
  val codec = int32.xmap(
    int2PgDate,
    pgDate2Int
  )

  private def int2PgDate(i: Int): PgDate = {
    PgDate(
      PgEpochTimestamp.plus(i.toLong, DAYS).atOffset(ZoneOffset.UTC).toLocalDate
    )
  }

  private def pgDate2Int(date: PgDate): Int = {
    val utcDate = date.value.atStartOfDay().atOffset(ZoneOffset.UTC).toInstant
    Duration.between(PgEpochTimestamp, utcDate).toDays.toInt
  }
}
