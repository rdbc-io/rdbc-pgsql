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

import io.rdbc.pgsql.core.messages.backend.ErrorMessage
import io.rdbc.pgsql.session.fsm.PgSession.Msg.Outbound.PgSessionError
import io.rdbc.pgsql.session.fsm.PgSession.PgStateData

object ConnectionDeactivatedBehavior {

  sealed trait StateData extends PgStateData

  object StateData {

    case class PgClosedConn(errorMessage: ErrorMessage) extends StateData

    case class UncategorizedClosedConn(msg: String) extends StateData

  }

}

class ConnectionDeactivatedBehavior(val session: PgSession) extends PgSessionBehavior {

  import ConnectionDeactivatedBehavior.StateData._
  import session._

  def behavior = {
    case Event(anyMsg, PgClosedConn(err)) =>
      sender() ! PgSessionError.PgReported(err)
      stay

    case Event(anyMsg, UncategorizedClosedConn(errMsg)) =>
      sender() ! PgSessionError.Uncategorized(errMsg)
      stay
  }
}
