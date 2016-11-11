import sbt._

object Version {
  val rdbc = "0.0.17"
  val netty = "4.1.6.Final"
}

object Library {
  val rdbcScalaApi = "io.rdbc" %% "rdbc-api-scala" % Version.rdbc
  val rdbcImplbase = "io.rdbc" %% "rdbc-implbase" % Version.rdbc
  val rdbcTypeconv = "io.rdbc" %% "rdbc-typeconv" % Version.rdbc
  val reactiveStreams = "org.reactivestreams" % "reactive-streams" % "1.0.0"
  val scodecBits = "org.scodec" %% "scodec-bits" % "1.1.2"
  val scodecCore = "org.scodec" %% "scodec-core" % "1.10.3"
  val scodecAkka = "org.scodec" %% "scodec-akka" % "0.2.0"
  val typesafeConfig = "com.typesafe" % "config" % "1.3.1"
  val nettyHandler = "io.netty" % "netty-handler" % Version.netty
  val nettyEpoll = "io.netty" % "netty-transport-native-epoll" % Version.netty classifier "linux-x86_64"
  val stm = "org.scala-stm" %% "scala-stm" % "0.7"
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
  val logback = "ch.qos.logback" % "logback-classic" % "1.1.7"
}