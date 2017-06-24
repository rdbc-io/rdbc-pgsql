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

package io.rdbc.pgsql.core.exception

import io.rdbc.api.exceptions.RdbcException
import io.rdbc.pgsql.core.pgstruct.StatusData

trait PgStatusDataException extends Throwable {
  def pgStatusData: StatusData
}

object PgStatusDataException {
  def apply(statusData: StatusData): RdbcException with PgStatusDataException = {
    if (statusData.sqlState == "42501")
      new PgUnauthorizedException(statusData)
    else if (statusData.sqlState == "57014")
      new PgTimeoutException(statusData)
    else if (statusData.sqlState.startsWith("28"))
      new PgAuthFailureException(statusData)
    else if (statusData.sqlState.startsWith("42"))
      new PgInvalidQueryException(statusData)
    else if (statusData.sqlState.startsWith("23"))
      new PgConstraintViolationException(statusData)
    else new PgUncategorizedStatusDataException(statusData)
  }
}
