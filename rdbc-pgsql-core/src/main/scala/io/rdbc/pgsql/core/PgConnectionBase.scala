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

package io.rdbc.pgsql.core

import io.rdbc.api.exceptions.IllegalSessionStateException
import io.rdbc.implbase.{ConnectionPartialImpl, ReturningInsertImpl}
import io.rdbc.sapi.{Connection, ReturningInsert}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait PgConnectionBase extends Connection with ConnectionPartialImpl {

  protected def simpleQueryIgnoreResult(sql: String)(implicit timeout: FiniteDuration): Future[Unit]

  def beginTx()(implicit timeout: FiniteDuration): Future[Unit] = simpleQueryIgnoreResult("BEGIN")
  def commitTx()(implicit timeout: FiniteDuration): Future[Unit] = simpleQueryIgnoreResult("COMMIT")
  def rollbackTx()(implicit timeout: FiniteDuration): Future[Unit] = simpleQueryIgnoreResult("ROLLBACK")
  def returningInsert(sql: String): Future[ReturningInsert] = returningInsert(sql, "*")
  def validate(): Future[Boolean] = simpleQueryIgnoreResult("")(FiniteDuration(100, "seconds")).map(_ => true).recoverWith {
    case ex: IllegalSessionStateException => Future.failed(ex)
    case _ => Future.successful(false) //TODO finite duration hardcoded here
  }

  def returningInsert(sql: String, keyColumns: String*): Future[ReturningInsert] = {
    val returningSql = sql + " returning " + keyColumns.mkString(",")
    statement(returningSql).map { stmt =>
      new ReturningInsertImpl(stmt)
    }
  }

}
