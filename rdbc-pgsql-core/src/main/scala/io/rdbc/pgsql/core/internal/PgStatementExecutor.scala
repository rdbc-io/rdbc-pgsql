/*
 * Copyright 2016 rdbc contributors
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

import io.rdbc.pgsql.core.SessionParams
import io.rdbc.pgsql.core.internal.protocol.Argument
import io.rdbc.pgsql.core.internal.protocol.messages.frontend.NativeSql
import io.rdbc.sapi.{RowPublisher, Timeout}
import org.reactivestreams.Publisher

import scala.concurrent.Future
import scala.util.Try

private[core] trait PgStatementExecutor {
  private[core] def statementStream(nativeSql: NativeSql, args: Vector[Argument])
                                   (implicit timeout: Timeout): RowPublisher

  private[core] def executeStatementForRowsAffected(nativeSql: NativeSql, args: Vector[Argument])
                                                   (implicit timeout: Timeout): Future[Long]

  private[core]
  def subscribeToStatementArgsStream[A](nativeStatement: PgNativeStatement,
                                        args: Publisher[A],
                                        argsConverter: A => Try[Vector[Argument]]): Future[Unit]

  private[core] def sessionParams: SessionParams
}
