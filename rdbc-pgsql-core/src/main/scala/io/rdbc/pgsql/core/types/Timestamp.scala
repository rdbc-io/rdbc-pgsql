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

package io.rdbc.pgsql.core.types

import java.time.LocalDateTime
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField

import io.rdbc.pgsql.core.SessionParams
import io.rdbc.pgsql.core.messages.data.Oid

trait Timestamp extends PgType[LocalDateTime] {
  //TODO pattern is dependent on some settings I guess
  val Pattern = {
    new DateTimeFormatterBuilder()
      .appendPattern("yyyy-MM-dd HH:mm:ss")
      .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
      .toFormatter()
  }

  val typeOid = Oid(1114)
  val cls = classOf[LocalDateTime]

  override def toObj(textualVal: String)(implicit sessionParams: SessionParams): LocalDateTime = LocalDateTime.parse(textualVal, Pattern)

  override def toPgTextual(obj: LocalDateTime)(implicit sessionParams: SessionParams): String = obj.format(Pattern)

}
