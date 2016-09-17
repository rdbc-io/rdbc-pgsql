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

import akka.actor.ActorRef
import io.rdbc.pgsql.core.messages.backend._
import io.rdbc.pgsql.core.messages.backend.auth.{AuthOk, AuthRequest}
import io.rdbc.pgsql.core.messages.frontend.StartupMessage
import io.rdbc.pgsql.session.fsm.PgSession.Msg.Inbound.StartSession
import io.rdbc.pgsql.session.fsm.PgSession.Msg.Outbound.{PgSessionError, Ready}
import io.rdbc.pgsql.session.fsm.PgSession.PgSessionState.SessionIdle
import io.rdbc.pgsql.session.fsm.PgSession.PgStateData
import io.rdbc.pgsql.transport.ConnectionManager._

object SessionInactiveBehavior {
  case class StateData(msg: StartupMessage, requester: ActorRef) extends PgStateData
}

class SessionInactiveBehavior(val session: PgSession) extends PgSessionBehavior {

  import SessionInactiveBehavior.StateData
  import session._

  def behavior = {
    case Event(StartSession(address, startupMessage), _) =>
      conn ! Connect(address)
      stay using StateData(startupMessage, sender())

    case Event(Connected, StateData(startupMsg, _)) =>
      conn ! Write(startupMsg)
      stay

    case Event(Received(AuthOk), _) =>
      stay

    case Event(Received(authReqMsg: AuthRequest), StateData(_, requester)) =>
      if (authenticator.supports(authReqMsg)) {
        val authAnswers = authenticator.authenticate(authReqMsg).answers
        conn ! Write(authAnswers: _*)
      } else {
        requester ! PgSessionError.Auth(s"The server requested to use '${authReqMsg.authMechanismName}' authentication mechanism, and provided authenticator does not support it.")
        conn ! Close
      }
      stay

    case Event(Received(err: ErrorMessage), StateData(_, requester)) =>
      if (err.statusData.sqlState.startsWith("28")) {
        requester ! PgSessionError.Auth(err.statusData.shortInfo)
      } else {
        requester ! PgSessionError.PgReported(err)
      }
      conn ! Close
      stay using PgStateData.NoData

    case Event(Received(keyData: BackendKeyData), _) =>
      maybeBackendKey = Some(keyData)
      stay

    case Event(Received(readyMsg@ReadyForQuery(IdleTxStatus)), StateData(_, requester)) =>
      requester ! Ready(readyMsg)
      goto(SessionIdle) using SessionIdleBehavior.StateData(IdleTxStatus)

    case Event(ConnectFailed, StateData(_, requester)) =>
      requester ! PgSessionError.Uncategorized("Failed to estabilish a TCP connection.")
      stay using PgStateData.NoData

    case Event(ConnectionClosed, data: StateData) =>
      stay using PgStateData.NoData
  }
}
