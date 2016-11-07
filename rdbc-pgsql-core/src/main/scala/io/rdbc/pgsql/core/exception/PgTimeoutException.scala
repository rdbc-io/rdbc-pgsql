package io.rdbc.pgsql.core.exception

import io.rdbc.api.exceptions.TimeoutException
import io.rdbc.pgsql.core.messages.backend.StatusData

class PgTimeoutException(val pgStatusData: StatusData) extends TimeoutException(pgStatusData.shortInfo) with PgStatusDataException
