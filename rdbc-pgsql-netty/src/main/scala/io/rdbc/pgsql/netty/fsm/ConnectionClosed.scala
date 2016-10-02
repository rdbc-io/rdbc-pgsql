package io.rdbc.pgsql.netty.fsm

import io.rdbc.api.exceptions.ConnectionClosedException
import io.rdbc.pgsql.core.messages.backend.PgBackendMessage
import io.rdbc.pgsql.netty.fsm.State.Outcome

case class ConnectionClosed(cause: ConnectionClosedException) extends State {
  override def handleMsg: PartialFunction[PgBackendMessage, Outcome] = PartialFunction.empty
  override def shortDesc: String = "connection_closed"
}
