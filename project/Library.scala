import sbt._

object Library {
  private val rdbcVersion = "0.0.82"
  private val reactiveStreamsVersion = "1.0.2"

  val rdbcScalaApi = "io.rdbc" %% "rdbc-api-scala" % rdbcVersion
  val rdbcJavaApi = "io.rdbc" % "rdbc-api-java" % rdbcVersion
  val rdbcImplbase = "io.rdbc" %% "rdbc-implbase" % rdbcVersion
  val rdbcUtil = "io.rdbc" %% "rdbc-util" % rdbcVersion
  val rdbcJavaAdapter = "io.rdbc" %% "rdbc-java-adapter" % rdbcVersion
  val reactiveStreams = "org.reactivestreams" % "reactive-streams" % reactiveStreamsVersion
  val scodecBits = "org.scodec" %% "scodec-bits" % "1.1.5"
  val scodecCore = "org.scodec" %% "scodec-core" % "1.10.3"
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
  val sourcecode = "com.lihaoyi" %% "sourcecode" % "0.1.4"
  val nettyHandler = "io.netty" % "netty-handler" % "4.1.25.Final"
  val stm = "org.scala-stm" %% "scala-stm" % "0.8"
  val immutables = "org.immutables" % "value" % "2.6.1"

  val pgsqljdbc = "org.postgresql" % "postgresql" % "42.2.2"

  val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
  val rdbcTck = "io.rdbc" %% "rdbc-tck" % rdbcVersion
  val scalatest = "org.scalatest" %% "scalatest" % "3.0.5"
  val pgsql = "ru.yandex.qatools.embed" % "postgresql-embedded" % "2.9"
  val reactiveStreamsTck = "org.reactivestreams" % "reactive-streams-tck" % reactiveStreamsVersion
}
