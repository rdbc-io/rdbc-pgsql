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

import akka.NotUsed
import akka.actor.{ActorLogging, ActorRef, FSM, Props}
import akka.stream.scaladsl.Source
import io.rdbc.ImmutSeq
import io.rdbc.pgsql.codec.{Decoder, Encoder}
import io.rdbc.pgsql.core.auth.Authenticator
import io.rdbc.pgsql.core.messages.backend._
import io.rdbc.pgsql.core.messages.frontend._
import io.rdbc.pgsql.session.fsm.PgSession.{PgSessionState, PgStateData}
import io.rdbc.pgsql.transport.akkaio.AkkaIoConnectionManager

import scala.concurrent.Future

object PgSession {
  def props(encoder: Encoder, decoder: Decoder, authenticator: Authenticator) = Props(classOf[PgSession], encoder, decoder, authenticator)

  sealed trait Msg
  object Msg {

    sealed trait Inbound
    object Inbound {
      case class StartSession(address: InetSocketAddress, startupMessage: StartupMessage) extends Inbound
      case class ParseAndExecuteStmt(parseMsg: Parse, bindMsg: Bind) extends Inbound
      case class ExecuteCachedStmt(bindMsg: Bind) extends Inbound
      case class ParseStmt(parseMsg: Parse) extends Inbound
      case class SubscribeBinds(source: Source[Bind, NotUsed])
      case object Cancel extends Inbound
    }

    sealed trait Outbound
    object Outbound {
      case class SourceRef(source: Source[DataRow, ActorRef], rowDesc: RowDescription, rowsAffected: Future[Long], warnings: Future[ImmutSeq[StatusMessage]]) extends Outbound
      case class Rows(rows: Vector[DescribedDataRow]) extends Outbound
      case class Ready(readyForQuery: ReadyForQuery) extends Outbound

      sealed trait PgSessionError extends Outbound
      object PgSessionError {
        case class PgReported(errorMsg: ErrorMessage) extends PgSessionError
        case class Auth(msg: String) extends PgSessionError
        case class SessionBusy(sessionStateName: String)
        case class Uncategorized(msg: String) extends PgSessionError
      }
    }
  }

  sealed trait PgSessionState
  object PgSessionState {
    case object SessionInactive extends PgSessionState
    case object SessionIdle extends PgSessionState
    case object QueryingSimple extends PgSessionState
    case object QueryingExtended extends PgSessionState
    case object SubscribedBinds extends PgSessionState
    case object Parsing extends PgSessionState
    case object ConnectionDeactivated extends PgSessionState
  }

  trait PgStateData
  object PgStateData {
    case object NoData extends PgStateData
  }
}

//TODO set supervisor strategy and generally familiarize yourself with supervision
class PgSession(
                 private[fsm] val encoder: Encoder,
                 private[fsm] val decoder: Decoder,
                 private[fsm] val authenticator: Authenticator
               ) extends FSM[PgSessionState, PgStateData] with ActorLogging {

  private[fsm] val conn = context.actorOf(AkkaIoConnectionManager.props(self, encoder, decoder), name = "connection")
  private[fsm] var maybeBackendKey: Option[BackendKeyData] = None
  implicit private[fsm] val ec = context.system.dispatcher

  import PgSession.PgSessionState._

  startWith(SessionInactive, PgStateData.NoData)

  when(SessionInactive)(new SessionInactiveBehavior(this).behavior)

  when(SessionIdle)(new SessionIdleBehavior(this).behavior)

  when(QueryingExtended)(new QueryingExtendedBehavior(this).behavior)

  when(QueryingSimple)(new QueryingSimpleBehavior(this).behavior)

  when(Parsing)(new ParsingBehavior(this).behavior)

  when(SubscribedBinds)(new SubscribedBindsBehavior(this).behavior)

  when(ConnectionDeactivated)(new ConnectionDeactivatedBehavior(this).behavior)

  whenUnhandled(new AnyStateBehavior(this).behavior)

  onTransition {
    case from -> to => log.debug(s"State transition from $from to $to")
  }
}



