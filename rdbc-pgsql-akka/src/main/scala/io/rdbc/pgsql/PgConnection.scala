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
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import io.rdbc.sapi._
import io.rdbc.api.exceptions.{BeginTxException, _}
import io.rdbc.implbase.{ConnectionPartialImpl, ReturningInsertImpl}
import io.rdbc.pgsql.core.PgTypeConvRegistry
import io.rdbc.pgsql.core.messages.backend.{ErrorMessage, ParseComplete}
import io.rdbc.pgsql.core.messages.frontend.{Parse, Query}
import io.rdbc.pgsql.exception.{PgParseEx, PgStmtExecutionEx}
import io.rdbc.pgsql.session.fsm.PgSession.Msg.Inbound.Cancel
import io.rdbc.pgsql.session.fsm.PgSession.Msg.Outbound.Rows
import io.rdbc.sapi.Connection
import org.reactivestreams.Publisher

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class PgConnection /*protected[pgsql]*/ (val rdbcTypeConvRegistry: TypeConverterRegistry, val pgTypeConvRegistry: PgTypeConvRegistry)(implicit sessionRef: SessionRef, system: ActorSystem, materializer: ActorMaterializer)
  extends Connection with ConnectionPartialImpl {

  private val stmtCache: mutable.Map[String, CachedPgStatement] = mutable.Map.empty
  //TODO replace with LRU map, remove unused statements from the db
  private val stmtParseCounter: mutable.Map[String, Int] = mutable.Map.empty.withDefaultValue(0)
  private var cachedStmtCounter: Int = 0

  implicit val ec: ExecutionContext = system.dispatcher

  override def beginTx()(implicit timeout: FiniteDuration): Future[Unit] = {
    executeSimpleStatement("BEGIN")
      .recover {
        case stmtErr: StmtExecutionException => BeginTxException(stmtErr.getMessage, Some(stmtErr))
      }
  }

  override def commitTx()(implicit timeout: FiniteDuration): Future[Unit] = {
    executeSimpleStatement("COMMIT")
      .recover {
        case stmtErr: StmtExecutionException => CommitTxException(stmtErr.getMessage, Some(stmtErr))
      }
  }
  override def rollbackTx()(implicit timeout: FiniteDuration): Future[Unit] = {
    executeSimpleStatement("ROLLBACK")
      .recover {
        case stmtErr: StmtExecutionException => RollbackTxException(stmtErr.getMessage, Some(stmtErr))
      }
  }
  override def release(): Future[Unit] = Future.successful(()) //TODO

  override def validate(): Future[Boolean] = ???

  override def statement(sql: String): Future[Statement] = {
    stmtCache.get(sql) match {
      case Some(cachedStmt) => Future.successful(cachedStmt)
      case None =>
        val parseCount = stmtParseCounter(sql) + 1
        stmtParseCounter.put(sql, parseCount)

        val nativeStmt = statement2Native(sql)

        //TODO threshold
        if (parseCount > 1) {
          stmtParseCounter.remove(sql)
          cachedStmtCounter += 1
          val stmtName = "S" + cachedStmtCounter
          implicit val timeout = Timeout(10, TimeUnit.DAYS) //TODO
          val parseResultFut = sessionRef.actor ? Parse(Some(stmtName), nativeStmt.statement, List.empty)

          parseResultFut.map {
            case ParseComplete =>
              val parsedStmt = new CachedPgStatement(stmtName, nativeStmt, rdbcTypeConvRegistry, pgTypeConvRegistry)
              stmtCache.put(sql, parsedStmt)
              new CachedPgStatement(stmtName, nativeStmt, rdbcTypeConvRegistry, pgTypeConvRegistry)

            case err: ErrorMessage => throw PgParseEx(err.statusData)
          }

        } else {
          Future.successful(new NotCachedPgStatement(nativeStmt, None, rdbcTypeConvRegistry, pgTypeConvRegistry))
        }
    }
  }

  override def returningInsert(sql: String): Future[ReturningInsert] = returningInsert(sql, "*")

  override def returningInsert(sql: String, keyColumns: String*): Future[ReturningInsert] = {
    val returningSql = sql + " returning " + keyColumns.mkString(",")
    statement(returningSql).map { stmt =>
      new ReturningInsertImpl(stmt)
    }
  }

  override def streamIntoTable(sql: String, paramsPublisher: Publisher[Map[String, Any]]): Future[Unit] = ???

  def executeSimpleStatement(sql: String)(implicit timeout: FiniteDuration): Future[Unit] = {
    val responseFut = akka.pattern.ask(sessionRef.actor, Query(sql))(Timeout(5 days)).flatMap {
      //TODO timeout
      case err: ErrorMessage => Future.failed(PgStmtExecutionEx(err.statusData))
      case Rows(_) => Future.successful(())
    } //TODO timeout handling cancel the query


    akka.pattern.after(timeout, using = system.scheduler) {
      if (!responseFut.isCompleted) {
        sessionRef.actor ! Cancel
      }
      Future.successful(())
    }

    responseFut
  }

  def statement2Native(statement: String): PgNativeStatement = {
    /* matches ":param1", ":param2" etc. and groups a match without an initial colon */
    val paramPattern =
    """:([^\W]*)""".r
    val params = paramPattern.findAllMatchIn(statement).map(_.group(1)).toVector
    val nativeStatement = params.zipWithIndex.foldLeft(statement) { (acc, elem) =>
      val (param, idx) = elem
      statement.replaceFirst(":" + param, "\\$" + (idx + 1))
    }
    PgNativeStatement(nativeStatement, params)
  }
}
