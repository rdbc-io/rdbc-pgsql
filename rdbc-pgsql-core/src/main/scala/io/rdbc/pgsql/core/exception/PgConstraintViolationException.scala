package io.rdbc.pgsql.core.exception

import io.rdbc.api.exceptions.ConstraintViolationException
import io.rdbc.pgsql.core.messages.backend.StatusData

class PgConstraintViolationException(val pgStatusData: StatusData)
  extends ConstraintViolationException(
    schema = pgStatusData.schemaName.getOrElse(""),
    table = pgStatusData.tableName.getOrElse(""),
    constraint = pgStatusData.constraintName.getOrElse(""),
    msg = pgStatusData.shortInfo
  ) with PgStatusDataException
