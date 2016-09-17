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

import akka.actor.{ActorRef, Status}
import akka.stream.scaladsl.Source
import io.rdbc.pgsql.core.messages.backend._
import io.rdbc.pgsql.core.messages.frontend.{PgFrontendMessage, Query}
import io.rdbc.pgsql.exception.PgStmtExecutionEx
import io.rdbc.pgsql.session.DataRowPublisher.Resume
import io.rdbc.pgsql.session.fsm.PgSession.Msg.Outbound.{PgSessionError, SourceRef}
import io.rdbc.pgsql.session.fsm.PgSession.PgSessionState.SessionIdle
import io.rdbc.pgsql.session.fsm.PgSession.PgStateData
import io.rdbc.pgsql.session.fsm.QueryingExtendedBehavior.StateData
import io.rdbc.pgsql.transport.ConnectionManager.{Received, Write}

import scala.concurrent.{Future, Promise}

object QueryingExtendedBehavior {

  sealed trait Stage

  object Stage {

    case class BeginningTx(msgsToSend: List[PgFrontendMessage]) extends Stage

    case object Binding extends Stage

    case object PullingRows extends Stage

    case object PullingComplete extends Stage

    case object PullingError extends Stage

    case object ComittingTx extends Stage

    case class RollingBackTx(error: ErrorMessage) extends Stage

    case class Errored(stage: Stage, error: ErrorMessage) extends Stage

  }

  case class StateData(requester: ActorRef,
                       source: Source[DataRow, ActorRef],
                       publisher: Future[ActorRef],
                       txManagement: Boolean,
                       stage: QueryingExtendedBehavior.Stage,
                       rowsAffected: Option[Long] = None,
                       commandCompletePromise: Promise[Long] = Promise[Long]) extends PgStateData

}

class QueryingExtendedBehavior(val session: PgSession) extends PgSessionBehavior {

  import QueryingExtendedBehavior.Stage._
  import session._

  def behavior = {
    case Event(Received(ParseComplete | BindComplete | PortalSuspended), _) =>
      stay

    //TODO on CloseComplete, there is no rowsAffected information

    case Event(Received(CommandComplete(message, maybeRowsAffected)), data: StateData) =>
      if (data.stage == PullingRows) {
        stay using data.copy(rowsAffected = maybeRowsAffected.map(_.toLong), stage = PullingComplete)
      } else {
        stay
      }

    case Event(Received(EmptyQueryResponse), data: StateData) =>
      //data.publisher.foreach(_ ! Status.Success(""))
      //stay using data.copy(operationComplete = true)
      stay //TODO

    case Event(Received(row: DataRow), data: StateData) =>
      data.publisher.foreach(_ ! row)
      stay

    case Event(Received(desc: RowDescription), data: StateData) =>
      data.requester ! SourceRef(data.source, desc, data.commandCompletePromise.future)
      stay using data.copy(stage = PullingRows)

    case Event(Received(io.rdbc.pgsql.core.messages.backend.NoData), data: StateData) => //TODO code dupl
      //TODO if you delete this case clause infinite loop will kick in, investigate this
      data.requester ! SourceRef(data.source, RowDescription(Vector.empty), data.commandCompletePromise.future)
      //TODO not pulling rows, stream is empty, complete it (what happens if you complete the stream before subscription?)
      stay using data.copy(stage = PullingRows)

    case Event(Received(ReadyForQuery(txStatus)), data: StateData) =>
      data.stage match {
        case PullingRows =>
          data.publisher.foreach(_ ! Resume)
          stay

        case BeginningTx(msgsToSend) =>
          conn ! Write(msgsToSend: _*)
          stay

        case PullingComplete if data.txManagement =>
          //TODO leverage "Show" typeclass in a logger
          log.debug("Pulling rows complete and tx management is enabled, committing tx")
          conn ! Write(Query("COMMIT"))
          stay using data.copy(stage = ComittingTx)

        case Errored(Binding | PullingRows, error) if data.txManagement =>
          log.debug("Pulling rows completed with an error and tx management is enabled, rolling back tx")
          conn ! Write(Query("ROLLBACK"))
          stay using data.copy(stage = RollingBackTx(error)) //TODO rollingBackTx has to carry data about stage at which error occurred. after the rollback if stage was "binding" requester has to be informed

        case PullingComplete =>
          data.publisher.foreach(_ ! Status.Success("")) //TODO ""
          data.commandCompletePromise.success(data.rowsAffected.getOrElse(0L))
          goto(SessionIdle) using SessionIdleBehavior.StateData(txStatus)

        case ComittingTx if txStatus == ActiveTxStatus =>
          stay //TODO do I have to do the same for the rollback?

        case ComittingTx if txStatus == IdleTxStatus =>
          data.publisher.foreach(_ ! Status.Success("")) //TODO ""
          data.commandCompletePromise.success(data.rowsAffected.getOrElse(0L))
          goto(SessionIdle) using SessionIdleBehavior.StateData(txStatus)

        case RollingBackTx(error) =>
          val ex = PgStmtExecutionEx(error.statusData)
          data.publisher.foreach(_ ! Status.Failure(ex))
          data.commandCompletePromise.failure(ex)
          goto(SessionIdle) using SessionIdleBehavior.StateData(txStatus)

        case Errored(RollingBackTx(error), _) =>
          //TODO error during rollback is swallowed, and cause of the rollback is sent to the client, decide what to do
          val ex = PgStmtExecutionEx(error.statusData)
          data.publisher.foreach(_ ! Status.Failure(ex))
          data.commandCompletePromise.failure(ex)
          goto(SessionIdle) using SessionIdleBehavior.StateData(txStatus)

        case Errored(Binding, error) =>
          data.publisher.foreach(_ ! Status.Failure(new Exception(""))) //TODO ex cause
          data.requester ! PgSessionError.PgReported(error)
          goto(SessionIdle) using SessionIdleBehavior.StateData(txStatus)

        case Errored(stage, error) =>
          val ex = PgStmtExecutionEx(error.statusData)
          data.publisher.foreach(_ ! Status.Failure(ex))
          data.commandCompletePromise.failure(ex)
          goto(SessionIdle) using SessionIdleBehavior.StateData(txStatus)
      }

    case Event(Received(err: ErrorMessage), data: StateData) =>
      stay using data.copy(stage = Errored(data.stage, err))

  }
}
