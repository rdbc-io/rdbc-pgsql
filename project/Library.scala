import sbt._

object Library {
  private val rdbcVersion = "0.0.72"

  val rdbcScalaApi = "io.rdbc" %% "rdbc-api-scala" % rdbcVersion
  val rdbcImplbase = "io.rdbc" %% "rdbc-implbase" % rdbcVersion
  val rdbcTypeconv = "io.rdbc" %% "rdbc-typeconv" % rdbcVersion
  val rdbcUtil = "io.rdbc" %% "rdbc-util" % rdbcVersion
  val reactiveStreams = "org.reactivestreams" % "reactive-streams" % "1.0.1"
  val scodecBits = "org.scodec" %% "scodec-bits" % "1.1.5"
  val scodecCore = "org.scodec" %% "scodec-core" % "1.10.3"
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
  val sourcecode = "com.lihaoyi" %% "sourcecode" % "0.1.4"
  val nettyHandler = "io.netty" % "netty-handler" % "4.1.16.Final"
  val stm = "org.scala-stm" %% "scala-stm" % "0.8"

  val pgsqljdbc = "org.postgresql" % "postgresql" % "42.1.4"

  val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
  val rdbcTck = "io.rdbc" %% "rdbc-tck" % rdbcVersion
  val scalatest = "org.scalatest" %% "scalatest" % "3.0.4"
  val pgsql = "ru.yandex.qatools.embed" % "postgresql-embedded" % "2.5"
  val reactiveStreamsTck = "org.reactivestreams" % "reactive-streams-tck" % "1.0.1"
}
