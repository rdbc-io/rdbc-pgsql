/*
 * Copyright 2016-2017 Krzysztof Pado
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

package io.rdbc.pgsql.core.internal

import io.rdbc.implbase.AnyParametrizedStatementPartialImpl
import io.rdbc.pgsql.core.pgstruct.ParamValue
import io.rdbc.pgsql.core.pgstruct.messages.frontend.NativeSql
import io.rdbc.sapi.{AnyParametrizedStatement, ResultSet, ResultStream}
import io.rdbc.util.Logging

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

private[core] class PgParametrizedStatement(executor: PgStatementExecutor,
                                            deallocator: PgStatementDeallocator,
                                            nativeSql: NativeSql,
                                            params: Vector[ParamValue])
                                           (implicit protected val ec: ExecutionContext)
  extends AnyParametrizedStatement
    with AnyParametrizedStatementPartialImpl
    with Logging {

  def deallocate(): Future[Unit] = traced {
    deallocator.deallocateStatement(nativeSql)
  }

  def executeForStream()(implicit timeout: FiniteDuration): Future[ResultStream] = traced {
    executor.executeStatementForStream(nativeSql, params)
  }

  override def executeForRowsAffected()(implicit timeout: FiniteDuration): Future[Long] = traced {
    executor.executeStatementForRowsAffected(nativeSql, params)
  }
}
