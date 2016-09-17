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
import io.rdbc.pgsql.session.fsm.PgSession.Msg.Outbound.PgSessionError
import io.rdbc.pgsql.session.fsm.PgSession.PgSessionState.SessionIdle
import io.rdbc.pgsql.session.fsm.PgSession.{PgSessionState, PgStateData}
import io.rdbc.pgsql.transport.ConnectionManager.Received

object ParsingBehavior {
  case class StateData(sender: ActorRef, error: Option[ErrorMessage] = None) extends PgStateData
}

class ParsingBehavior(val session: PgSession) extends PgSessionBehavior {

  import ParsingBehavior.StateData
  import session._

  def behavior = {
    case Event(Received(ReadyForQuery(txStatus)), data: StateData) =>
      data.error match {
        case None =>
          log.debug(s"Ready for query received after a successful parse with tx status $txStatus")
          data.sender ! ParseComplete

        case Some(err) =>
          log.debug(s"Ready for query received after a failed parse with tx status $txStatus")
          data.sender ! PgSessionError.PgReported(err)
      }
      goto(SessionIdle) using SessionIdleBehavior.StateData(txStatus)

    case Event(Received(ParseComplete), _) =>
      log.debug("Parse command completed")
      stay

    case Event(Received(err: ErrorMessage), data: StateData) =>
      log.error(s"Error ${err.statusData.shortInfo} occurred when parsing")
      stay using data.copy(error = Some(err))
  }
}
