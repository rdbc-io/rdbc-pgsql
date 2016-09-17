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

package io.rdbc.pgsql.session.fsm

import java.net.InetSocketAddress

import io.rdbc.pgsql.core.PgCharset
import io.rdbc.pgsql.core.messages.backend._
import io.rdbc.pgsql.core.messages.frontend.{CancelRequest, ClientCharset}
import io.rdbc.pgsql.session.RequestCanceller
import io.rdbc.pgsql.session.fsm.ConnectionDeactivatedBehavior.{StateData => ConnDeactData}
import io.rdbc.pgsql.session.fsm.PgSession.Msg.Inbound.Cancel
import io.rdbc.pgsql.session.fsm.PgSession.Msg.Outbound.PgSessionError
import io.rdbc.pgsql.session.fsm.PgSession.PgSessionState.ConnectionDeactivated
import io.rdbc.pgsql.transport.ConnectionManager.{Close, ConnectionClosed, Received}

class AnyStateBehavior(val session: PgSession) extends PgSessionBehavior {

  import session._

  def behavior = {
    case Event(Received(ParameterStatus("client_encoding", encoding)), _) =>
      PgCharset.toJavaCharset(encoding) match {
        case Some(javaCharset) =>
          conn ! ClientCharset(javaCharset)
          stay

        case None =>
          goto(ConnectionDeactivated) using ConnDeactData.UncategorizedClosedConn(s"Requested client encoding $encoding is not supported")
      }

    case Event(Received(ParameterStatus("server_encoding", encoding)), _) =>
      PgCharset.toJavaCharset(encoding) match {
        case Some(javaCharset) =>
          conn ! ServerCharset(javaCharset)
          stay

        case None =>
          goto(ConnectionDeactivated) using ConnDeactData.UncategorizedClosedConn(s"Requested server encoding $encoding is not supported")
      }

    case Event(Received(msg: ParameterStatus), _) =>
      println(s"parameter status $msg") //TODO
      stay

    case Event(Received(msg: UnknownPgMessage), _) =>
      log.error(s"Unknown message received: $msg")
      conn ! Close
      goto(ConnectionDeactivated)
        .using(ConnDeactData.UncategorizedClosedConn(s"Unknown PG message received ($msg)"))

    case Event(ConnectionClosed, _) =>
      goto(ConnectionDeactivated)
        .using(ConnDeactData.UncategorizedClosedConn("TCP connection has been closed with no additional information"))

    //TODO on unhandled event Received - fatal error

    case Event(Received(err: ErrorMessage), _) =>
      conn ! Close
      goto(ConnectionDeactivated) using ConnDeactData.PgClosedConn(err)

    case Event(Cancel, _) =>
      maybeBackendKey.foreach { backendKey =>
        val address = InetSocketAddress.createUnresolved("localhost", 5432)
        //TODO
        val canceller = context.actorOf(RequestCanceller.props(address, encoder, decoder), name = "requestCanceller")
        canceller ! CancelRequest(backendKey.pid, backendKey.key)
      }
      stay

    case Event(anyMsg: PgSession.Msg.Inbound, _) =>
      log.debug("Received message {} in {} state, session is busy now.", anyMsg, stateName)
      stay replying PgSessionError.SessionBusy(stateName.toString) //TODO .show

    case Event(Received(notice: NoticeMessage), _) =>
      log.debug(s"Notice message received: ${notice.statusData.shortInfo}")
      stay
  }
}
