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

package io.rdbc.pgsql.core.exception

import io.rdbc.api.exceptions.ParseException
import io.rdbc.api.exceptions.ParseException.{SyntaxErrorException, UncategorizedParseException}
import io.rdbc.pgsql.core.messages.backend.StatusData

object PgParseException {
  def apply(statusData: StatusData): ParseException = {
    if (statusData.sqlState.startsWith("42")) new PgSyntaxErrorException(statusData)
    else new PgUncategorizedParseException(statusData)
  }
}

class PgSyntaxErrorException(val pgStatusData: StatusData) extends SyntaxErrorException(pgStatusData.shortInfo, pgStatusData.position) with PgException

class PgUncategorizedParseException(val pgStatusData: StatusData) extends UncategorizedParseException(pgStatusData.shortInfo, pgStatusData.detail) with PgException
