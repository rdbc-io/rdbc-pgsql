package io.rdbc.pgsql.core.exception

import io.rdbc.api.exceptions.AuthFailureException
import io.rdbc.pgsql.core.messages.backend.StatusData

class PgAuthFailureException(val pgStatusData: StatusData) extends AuthFailureException(pgStatusData.shortInfo) with PgStatusDataException
