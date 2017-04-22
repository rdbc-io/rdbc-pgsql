package io.rdbc.pgsql.transport.netty

import de.flapdoodle.embed.process.distribution.IVersion
import io.rdbc.sapi.Connection
import io.rdbc.test._
import org.reactivestreams.tck.TestEnvironment
import org.testng.annotations.{AfterClass, BeforeClass}
import ru.yandex.qatools.embed.postgresql.config.AbstractPostgresConfig.{Credentials, Net, Storage}
import ru.yandex.qatools.embed.postgresql.config.{AbstractPostgresConfig, PostgresConfig}
import ru.yandex.qatools.embed.postgresql.distribution.Version
import ru.yandex.qatools.embed.postgresql.distribution.Version.Main.PRODUCTION
import ru.yandex.qatools.embed.postgresql.{PostgresProcess, PostgresStarter}

import scala.collection.JavaConverters._

class PgRowPublisherVerification
  extends RowPublisherVerification(new TestEnvironment, 1000L) {
  @volatile var connFact: NettyPgConnectionFactory = _
  @volatile var pgsqlProcess: PostgresProcess = _

  private val pgVersion: IVersion = {
    val verStr = Option(System.getenv("PG_VER")).getOrElse(PRODUCTION.name())
    Version.Main.valueOf(verStr)
  }

  @BeforeClass
  def beforeClass(): Unit = {
    val pgsqlStarter = PostgresStarter.getDefaultInstance
    val pgsqlCfg = new PostgresConfig(
      pgVersion,
      new Net,
      new Storage("rdbc"),
      new AbstractPostgresConfig.Timeout,
      new Credentials("rdbc", "rdbc")
    )
    pgsqlCfg.getAdditionalInitDbParams.addAll(Vector(
      "-E", "UTF-8",
      "--locale=en_US.UTF-8",
      "--lc-collate=en_US.UTF-8",
      "--lc-ctype=en_US.UTF-8").asJavaCollection
    )

    val pgsqlExec = pgsqlStarter.prepare(pgsqlCfg)
    pgsqlProcess = pgsqlExec.start

    connFact = NettyPgConnectionFactory(
      pgsqlCfg.net().host(),
      pgsqlCfg.net().port(),
      pgsqlCfg.credentials().username(),
      pgsqlCfg.credentials().password()
    )
  }

  @AfterClass
  def afterClass(): Unit = {
    pgsqlProcess.stop()
    connFact.shutdown().get
  }

  protected def connection(): Connection = connFact.connection().get

  protected val intDataType: String = "int4"
  protected val varcharDataType: String = "varchar"
  protected def castVarchar2Int(colName: String): String = s"cast ($colName as int4)"
}
