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

package io.rdbc.pgsql.transport.netty

import io.rdbc.sapi.Connection
import io.rdbc.tck._
import org.reactivestreams.tck.TestEnvironment
import org.testng.annotations.{AfterClass, BeforeClass}

class PgRowPublisherVerification
  extends RowPublisherVerification(new TestEnvironment, 1000L) {
  @volatile var postgres: PostgresProcess = _

  @BeforeClass
  def beforeClass(): Unit = {
    postgres = Postgres.start()
  }

  @AfterClass
  def afterClass(): Unit = {
    postgres.stop()
  }

  protected def connection(): Connection = postgres.connFact.connection().get

  protected val intDataType: String = "int4"
  protected val varcharDataType: String = "varchar"
  protected def castVarchar2Int(colName: String): String = s"cast ($colName as int4)"
}
