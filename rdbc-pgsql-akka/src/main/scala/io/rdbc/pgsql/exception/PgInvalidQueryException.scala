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

package io.rdbc.pgsql.exception

import io.rdbc.api.exceptions.ConnectException.{AuthFailureException, UncategorizedConnectException}
import io.rdbc.api.exceptions.ParseException.{SyntaxErrorException, UncategorizedParseException}
import io.rdbc.api.exceptions._
import io.rdbc.api.exceptions.StmtExecutionException._
import io.rdbc.pgsql.core.messages.backend.StatusData

trait PgEx {
  def pgStatusData: StatusData
}

object PgStmtExecutionEx {
  def apply(statusData: StatusData): RdbcException = {
    if (statusData.sqlState == "42501") new PgUnauthorizedException(statusData)
    else if (statusData.sqlState == "57014") new PgTimeoutException(statusData)
    else if (statusData.sqlState.startsWith("42")) new PgInvalidQueryException(statusData)
    else if (statusData.sqlState.startsWith("23")) new PgConstraintViolationException(statusData)
    else new PgUncategorizedExecutionException(statusData)
  }
}

class PgInvalidQueryException(val pgStatusData: StatusData) extends InvalidQueryException(pgStatusData.shortInfo, pgStatusData.position) with PgEx

class PgUnauthorizedException(val pgStatusData: StatusData) extends UnauthorizedException(pgStatusData.shortInfo) with PgEx

class PgConstraintViolationException(val pgStatusData: StatusData)
  extends ConstraintViolationException(
    schema = pgStatusData.schemaName.getOrElse(""),
    table = pgStatusData.tableName.getOrElse(""),
    constraint = pgStatusData.constraintName.getOrElse(""),
    msg = pgStatusData.shortInfo
  ) with PgEx

class PgTimeoutException(val pgStatusData: StatusData) extends TimeoutException(pgStatusData.shortInfo) with PgEx

class PgUncategorizedExecutionException(val pgStatusData: StatusData) extends UncategorizedExecutionException(pgStatusData.shortInfo, pgStatusData.detail) with PgEx


object PgParseEx {
  def apply(statusData: StatusData): ParseException = {
    if (statusData.sqlState.startsWith("42")) new PgSyntaxErrorException(statusData)
    else new PgUncategorizedParseException(statusData)
  }
}

class PgSyntaxErrorException(val pgStatusData: StatusData) extends SyntaxErrorException(pgStatusData.shortInfo, pgStatusData.position) with PgEx

class PgUncategorizedParseException(val pgStatusData: StatusData) extends UncategorizedParseException(pgStatusData.shortInfo, pgStatusData.detail) with PgEx

object PgConnectEx {
  def apply(statusData: StatusData): ConnectException = {
    if (statusData.sqlState.startsWith("28")) new PgAuthFailureException(statusData)
    else new PgUncategorizedConnectException(statusData)
  }
}

class PgAuthFailureException(val pgStatusData: StatusData) extends AuthFailureException(pgStatusData.shortInfo)
class PgUncategorizedConnectException(val pgStatusData: StatusData) extends UncategorizedConnectException(pgStatusData.shortInfo, pgStatusData.detail)