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
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration.FiniteDuration

class PgRdbcSuite extends RdbcSuite with BeforeAndAfterAll {
  @volatile var postgres: PostgresProcess = _

  override def beforeAll: Unit = {
    postgres = Postgres.start()
  }

  override def afterAll: Unit = {
    postgres.stop()
  }

  protected def connection(): Connection = postgres.connFact.connection().get

  protected lazy val intDataTypeName = "int4"
  protected lazy val intDataTypeId = "23"
  protected lazy val arbitraryDataType: String = intDataTypeName

  protected def slowStatement(time: FiniteDuration): String = s"select pg_sleep(${time.toSeconds})"
}
