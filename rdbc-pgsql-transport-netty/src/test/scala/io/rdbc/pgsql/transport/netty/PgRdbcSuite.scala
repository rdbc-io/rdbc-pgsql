package io.rdbc.pgsql.transport.netty

import io.rdbc.sapi.Connection
import io.rdbc.test._
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration.FiniteDuration

class PgRdbcSuite extends RdbcSuite with BeforeAndAfterAll {
  @volatile var connFact: NettyPgConnectionFactory = _

  override def beforeAll: Unit = {
    connFact = NettyPgConnectionFactory("localhost", 5432, "postgres", "")
  }

  override def afterAll: Unit = {
    connFact.shutdown().get
  }

  protected def connection(): Connection = connFact.connection().get
  protected val arbitraryDataType: String = "int4"
  protected def slowStatement(time: FiniteDuration): String = s"select pg_sleep(${time.toSeconds})"
}
