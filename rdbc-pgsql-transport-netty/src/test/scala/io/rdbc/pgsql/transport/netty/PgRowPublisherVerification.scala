package io.rdbc.pgsql.transport.netty

import io.rdbc.sapi.Connection
import io.rdbc.test._
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
