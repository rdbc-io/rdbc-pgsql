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
import io.rdbc.pgsql.session.fsm.PgSession.Msg.Outbound.{PgSessionError, Rows}
import io.rdbc.pgsql.session.fsm.PgSession.PgSessionState.SessionIdle
import io.rdbc.pgsql.session.fsm.PgSession.PgStateData
import io.rdbc.pgsql.transport.ConnectionManager.Received

object QueryingSimpleBehavior {

  case class StateData(sender: ActorRef, dataRows: Vector[DescribedDataRow], rowDesc: Option[RowDescription] = None, err: Option[ErrorMessage] = None) extends PgStateData

}

class QueryingSimpleBehavior(val session: PgSession) extends PgSessionBehavior {

  import QueryingSimpleBehavior.StateData
  import session._

  def behavior = {
    case Event(Received(rowDesc: RowDescription), queryData: StateData) =>
      stay using queryData.copy(rowDesc = Some(rowDesc))

    case Event(Received(dataRow: DataRow), qd@StateData(querySender, dataRows, Some(rowDesc), _)) =>
      stay using qd.copy(dataRows = qd.dataRows :+ dataRow.describe(rowDesc))

    case Event(Received(ready: ReadyForQuery), StateData(querySender, rows, _, None)) =>
      querySender ! Rows(rows)
      goto(SessionIdle) using SessionIdleBehavior.StateData(ready.txStatus)

    case Event(Received(ready: ReadyForQuery), StateData(querySender, _, _, Some(err))) =>
      querySender ! PgSessionError.PgReported(err)
      goto(SessionIdle) using SessionIdleBehavior.StateData(ready.txStatus)

    case Event(Received(CommandComplete(_, _)), _) =>
      stay

    case Event(Received(err: ErrorMessage), qd: StateData) =>
      stay using qd.copy(err = Some(err))
  }
}
