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

package io.rdbc.pgsql.core.internal.typecodec

import java.time._

package object sco {

  private[sco] implicit class Duration2Micros(underlying: Duration) {
    def toMicros: Long = {
      (underlying.getSeconds * 1000L * 1000L) + (underlying.getNano / 1000L)
    }
  }

  private[sco] val PgEpochTime = LocalTime.MIDNIGHT

  private[sco] val PgEpochTimestamp: Instant = {
    LocalDate.of(2000, Month.JANUARY, 1)
      .atTime(PgEpochTime)
      .atOffset(ZoneOffset.UTC)
      .toInstant
  }
}
