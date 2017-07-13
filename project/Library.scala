import sbt._

object Library {
  private val rdbcVersion = "0.0.66"

  val rdbcScalaApi = "io.rdbc" %% "rdbc-api-scala" % rdbcVersion
  val rdbcImplbase = "io.rdbc" %% "rdbc-implbase" % rdbcVersion
  val rdbcTypeconv = "io.rdbc" %% "rdbc-typeconv" % rdbcVersion
  val rdbcUtil = "io.rdbc" %% "rdbc-util" % rdbcVersion
  val reactiveStreams = "org.reactivestreams" % "reactive-streams" % "1.0.0"
  val scodecBits = "org.scodec" %% "scodec-bits" % "1.1.4"
  val scodecCore = "org.scodec" %% "scodec-core" % "1.10.3"
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
  val sourcecode = "com.lihaoyi" %% "sourcecode" % "0.1.3"
  val nettyHandler = "io.netty" % "netty-handler" % "4.1.11.Final"
  val stm = "org.scala-stm" %% "scala-stm" % "0.8"

  val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
  val rdbcTests = "io.rdbc" %% "rdbc-tests" % rdbcVersion
  val scalatest = "org.scalatest" %% "scalatest" % "3.0.3"
  val pgsql = "ru.yandex.qatools.embed" % "postgresql-embedded" % "2.1"
  val reactiveStreamsTck = "org.reactivestreams" % "reactive-streams-tck" % "1.0.0"
}
