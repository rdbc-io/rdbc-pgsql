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
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import io.rdbc.pgsql.core.messages.backend._
import io.rdbc.pgsql.core.messages.frontend._
import io.rdbc.pgsql.session.{BindSubscriber, DataRowPublisher}
import io.rdbc.pgsql.session.fsm.PgSession.Msg.Inbound.{ExecuteCachedStmt, ParseAndExecuteStmt, SubscribeBinds}
import io.rdbc.pgsql.session.fsm.PgSession.PgSessionState.{Parsing, QueryingExtended, QueryingSimple, SubscribedBinds}
import io.rdbc.pgsql.session.fsm.PgSession.PgStateData
import io.rdbc.pgsql.session.fsm.QueryingExtendedBehavior.Stage.{BeginningTx, Binding}
import io.rdbc.pgsql.transport.ConnectionManager.Write

import scala.concurrent.Promise
import scala.util.Try

object SessionIdleBehavior {
  case class StateData(txStatus: TxStatus) extends PgStateData
}

class SessionIdleBehavior(val session: PgSession) extends PgSessionBehavior {

  import SessionIdleBehavior.StateData
  import session._

  def behavior = {
    case Event(ParseAndExecuteStmt(parse, bind), data: StateData) =>
      val describe = Describe(PreparedStatementType, parse.optionalName)

      val sourceActorPromise = Promise[ActorRef]
      //TODO this promise probably breaks good practices, actors should communicate via messages only, this promise effectively allows to communicate via it
      val rowsSource =
        Source.actorPublisher[DataRow](DataRowPublisher.props(conn, bind.portal))
          .mapMaterializedValue { act =>
            sourceActorPromise.complete(Try(act))
            act
          }

      if (data.txStatus == IdleTxStatus) {
        conn ! Write(Query("BEGIN"))
        goto(QueryingExtended) using QueryingExtendedBehavior.StateData(sender(), rowsSource, sourceActorPromise.future, txManagement = true, stage = BeginningTx(List(parse, bind, describe, Sync)))
      } else {
        conn ! Write(parse, bind, describe, Sync)
        goto(QueryingExtended) using QueryingExtendedBehavior.StateData(sender(), rowsSource, sourceActorPromise.future, txManagement = false, stage = Binding)
      }

    case Event(ExecuteCachedStmt(bind), data: StateData) =>
      //TODO massive code dupl
      val describe = Describe(PreparedStatementType, bind.preparedStmt)

      val sourceActorPromise = Promise[ActorRef]
      //TODO this promise probably breaks good practices, actors should communicate via messages only, this promise effectively allows to communicate via it
      val rowsSource =
        Source.actorPublisher[DataRow](DataRowPublisher.props(conn, bind.portal))
          .mapMaterializedValue { act =>
            sourceActorPromise.complete(Try(act))
            act
          }

      if (data.txStatus == IdleTxStatus) {
        conn ! Write(Query("BEGIN"))
        goto(QueryingExtended) using QueryingExtendedBehavior.StateData(sender(), rowsSource, sourceActorPromise.future, txManagement = true, stage = BeginningTx(List(bind, describe, Sync)))
      } else {
        conn ! Write(bind, describe, Sync)
        goto(QueryingExtended) using QueryingExtendedBehavior.StateData(sender(), rowsSource, sourceActorPromise.future, txManagement = false, stage = Binding)
      }

    case Event(SubscribeBinds(source), data: StateData) =>
      implicit val materializer = ActorMaterializer() //TODO
      source.runWith(Sink.actorSubscriber(BindSubscriber.props))
      goto(SubscribedBinds)

    case Event(msg: Parse, data: StateData) =>
      conn ! Write(msg, Sync)
      goto(Parsing) using ParsingBehavior.StateData(sender())

    case Event(msg: Query, _) =>
      conn ! Write(msg)
      goto(QueryingSimple) using QueryingSimpleBehavior.StateData(sender(), Vector.empty)
  }
}
