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

import com.typesafe.scalalogging.StrictLogging
import io.rdbc.ImmutSeq
import io.rdbc.implbase.AnyParametrizedStatementPartialImpl
import io.rdbc.pgsql.core.messages.frontend._
import io.rdbc.sapi.{AnyParametrizedStatement, ResultStream}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

class PgParametrizedStatement(executor: PgStatementExecutor,
                              deallocator: PgStatementDeallocator,
                              nativeSql: String,
                              params: ImmutSeq[DbValue])
                             (implicit val ec: ExecutionContext)
  extends AnyParametrizedStatement
    with AnyParametrizedStatementPartialImpl
    with StrictLogging {

  def deallocate(): Future[Unit] = deallocator.deallocateStatement(nativeSql)
  def executeForStream()(implicit timeout: FiniteDuration): Future[ResultStream] = executor.executeStatementForStream(nativeSql, params)
  override def executeForRowsAffected()(implicit timeout: FiniteDuration): Future[Long] = executor.executeStatementForRowsAffected(nativeSql, params)
}
