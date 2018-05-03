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
import java.time.temporal.ChronoUnit.MICROS

import io.rdbc.pgsql.core.types.{PgTimestampTz, PgTimestampTzType}
import scodec.codecs.int64

private[typecodec] object ScodecPgTimestampTzCodec
  extends ScodecPgValCodec[PgTimestampTz]
    with IgnoreSessionParams[PgTimestampTz] {

  val typ = PgTimestampTzType
  val codec = int64.xmap(
    long2pgTimestampTz,
    pgTimestampTz2Long
  )

  private def long2pgTimestampTz(l: Long): PgTimestampTz = {
    PgTimestampTz(
      PgEpochTimestamp.plus(l, MICROS).atZone(ZoneId.systemDefault())
      //TODO should probably use timezone session param for this
    )
  }

  private def pgTimestampTz2Long(timestamp: PgTimestampTz): Long = {
    Duration.between(PgEpochTimestamp, timestamp.value).toMicros
  }
}
