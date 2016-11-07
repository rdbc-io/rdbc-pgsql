package io.rdbc.pgsql.core.exception

import io.rdbc.api.exceptions.InvalidQueryException
import io.rdbc.pgsql.core.messages.backend.StatusData

class PgInvalidQueryException(val pgStatusData: StatusData) extends InvalidQueryException(pgStatusData.shortInfo, pgStatusData.position) with PgStatusDataException
