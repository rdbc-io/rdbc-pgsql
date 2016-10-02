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

package io.rdbc.pgsql.netty

import io.rdbc.ImmutIndexedSeq
import io.rdbc.implbase.BindablePartialImpl
import io.rdbc.pgsql.core.messages.frontend.DbValue
import io.rdbc.pgsql.core.{PgNativeStatement, PgStatement}
import io.rdbc.sapi.{ParametrizedStatement, Statement}

class PgNettyStatement(conn: PgNettyConnection, val nativeStmt: PgNativeStatement)
  extends Statement
    with BindablePartialImpl[ParametrizedStatement]
    with PgStatement {

  val pgTypeRegistry = conn.pgTypeConvRegistry
  def sessionParams = conn.sessionParams

  protected def parametrizedStmt(dbValues: ImmutIndexedSeq[DbValue]): ParametrizedStatement = {
    new PgNettyParametrizedStatement(conn, nativeStmt.statement, dbValues)
  }
}
