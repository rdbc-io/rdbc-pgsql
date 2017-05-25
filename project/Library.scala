import sbt._

object Library {
  private val rdbcVersion = "0.0.59"
  private val nettyVersion = "4.0.46.Final"

  val rdbcScalaApi = "io.rdbc" %% "rdbc-api-scala" % rdbcVersion
  val rdbcImplbase = "io.rdbc" %% "rdbc-implbase" % rdbcVersion
  val rdbcTypeconv = "io.rdbc" %% "rdbc-typeconv" % rdbcVersion
  val rdbcUtil = "io.rdbc" %% "rdbc-util" % rdbcVersion
  val reactiveStreams = "org.reactivestreams" % "reactive-streams" % "1.0.0"
  val akkaStream = "com.typesafe.akka" %% "akka-stream" % "2.4.18"
  val scodecBits = "org.scodec" %% "scodec-bits" % "1.1.4"
  val scodecCore = "org.scodec" %% "scodec-core" % "1.10.3"
  val typesafeConfig = "com.typesafe" % "config" % "1.3.1"
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
  val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
  val sourcecode = "com.lihaoyi" %% "sourcecode" % "0.1.3"
  val nettyHandler = "io.netty" % "netty-handler" % nettyVersion
  val nettyEpoll = "io.netty" % "netty-transport-native-epoll" % nettyVersion classifier "linux-x86_64"

  val rdbcTests = "io.rdbc" %% "rdbc-tests" % rdbcVersion % Test
  val scalatest = "org.scalatest" %% "scalatest" % "3.0.3" % Test
  val pgsql = "ru.yandex.qatools.embed" % "postgresql-embedded" % "2.1" % Test
}
