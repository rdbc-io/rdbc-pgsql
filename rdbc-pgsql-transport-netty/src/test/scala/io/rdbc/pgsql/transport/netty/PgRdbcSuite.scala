package io.rdbc.pgsql.transport.netty

import io.rdbc.sapi.Connection
import io.rdbc.test._
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

  protected val intDataTypeName = "int4"
  protected val intDataTypeId = "23"
  protected val arbitraryDataType: String = intDataTypeName

  protected def slowStatement(time: FiniteDuration): String = s"select pg_sleep(${time.toSeconds})"
}
