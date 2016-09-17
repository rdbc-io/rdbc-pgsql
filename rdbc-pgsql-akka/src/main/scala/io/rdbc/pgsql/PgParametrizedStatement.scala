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

package io.rdbc.pgsql

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import io.rdbc.core.api.exceptions.StmtExecutionException.UncategorizedExecutionException
import io.rdbc.core.api.{ParametrizedStatement, ResultStream}
import io.rdbc.pgsql.core.PgTypeConvRegistry
import io.rdbc.pgsql.core.messages.frontend._
import io.rdbc.pgsql.exception.PgStmtExecutionEx
import io.rdbc.pgsql.session.fsm.PgSession.Msg.Inbound.{Cancel, ExecuteCachedStmt, ParseAndExecuteStmt}
import io.rdbc.pgsql.session.fsm.PgSession.Msg.Outbound.{PgSessionError, SourceRef}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

trait PgParametrizedStatement extends ParametrizedStatement {

  override implicit val ec: ExecutionContext = system.dispatcher

  def requestMsg: Any

  implicit def sessionRef: SessionRef

  implicit def system: ActorSystem

  implicit def materializer: ActorMaterializer

  def typeConvRegistry: PgTypeConvRegistry

  override def executeForStream()(implicit timeout: FiniteDuration): Future[ResultStream] = {

    //TODO this is a hard timeout that causes an error if PG doesn't cancel the query
    val askResult = akka.pattern.ask(sessionRef.actor, requestMsg)(akka.util.Timeout(100, TimeUnit.DAYS)).flatMap {
      case PgSessionError.PgReported(err) => Future.failed(PgStmtExecutionEx(err.statusData))
      case PgSessionError.Uncategorized(errMsg) => Future.failed(UncategorizedExecutionException(errMsg))
      case sref: SourceRef => Future.successful(new PgResultStream(sref, typeConvRegistry))
    }

    //TODO this is a soft timeout that asks PG to cancel the query
    akka.pattern.after(timeout, using = system.scheduler) {
      if (!askResult.isCompleted) {
        sessionRef.actor ! Cancel
      }
      Future.successful()
    }

    askResult

  }
}

class NotCachedPgParametrizedStatement(val sql: String, val stmtName: Option[String], val params: IndexedSeq[DbValue], val typeConvRegistry: PgTypeConvRegistry)(implicit val sessionRef: SessionRef, val system: ActorSystem, val materializer: ActorMaterializer) extends PgParametrizedStatement {
  val requestMsg: Any = ParseAndExecuteStmt(Parse(stmtName, sql, List.empty), Bind(stmtName.map(_ + "_portal"), stmtName, params.toList, AllTextual)) //TODO toList, //TODO AllTextual
}

class CachedPgParametrizedStatement(val stmtName: String, val params: IndexedSeq[DbValue], val typeConvRegistry: PgTypeConvRegistry)(implicit val sessionRef: SessionRef, val system: ActorSystem, val materializer: ActorMaterializer) extends PgParametrizedStatement {
  val requestMsg: Any = ExecuteCachedStmt(Bind(Some(s"${stmtName}_portal"), Some(stmtName), params.toList, AllTextual)) //TODO toList, //TODO AllTextual
}