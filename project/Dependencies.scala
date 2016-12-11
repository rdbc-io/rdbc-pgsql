import sbt._

object Version {
  val rdbc = "0.0.23"
  val netty = "4.1.6.Final"
}

object Library {
  val rdbcScalaApi = "io.rdbc" %% "rdbc-api-scala" % Version.rdbc
  val rdbcImplbase = "io.rdbc" %% "rdbc-implbase" % Version.rdbc
  val rdbcTypeconv = "io.rdbc" %% "rdbc-typeconv" % Version.rdbc
  val reactiveStreams = "org.reactivestreams" % "reactive-streams" % "1.0.0"
  val akkaStream  = "com.typesafe.akka" %% "akka-stream" % "2.4.14"
  val scodecBits = "org.scodec" %% "scodec-bits" % "1.1.2"
  val scodecCore = "org.scodec" %% "scodec-core" % "1.10.3"
  val typesafeConfig = "com.typesafe" % "config" % "1.3.1"
  val nettyHandler = "io.netty" % "netty-handler" % Version.netty
  val nettyEpoll = "io.netty" % "netty-transport-native-epoll" % Version.netty classifier "linux-x86_64"
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
  val logback = "ch.qos.logback" % "logback-classic" % "1.1.8"
}
