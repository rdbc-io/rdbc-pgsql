package io.rdbc.pgsql.core.exception

import io.rdbc.api.exceptions.UnauthorizedException
import io.rdbc.pgsql.core.messages.backend.StatusData

class PgUnauthorizedException(val pgStatusData: StatusData) extends UnauthorizedException(pgStatusData.shortInfo) with PgStatusDataException
