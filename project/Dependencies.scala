import sbt._

object Version {
  val rdbc = "0.0.6"
}

object Library {
  val rdbcScalaApi = "io.rdbc" %% "rdbc-api-scala" % Version.rdbc
  val rdbcImplbase = "io.rdbc" %% "rdbc-implbase" % Version.rdbc
  val rdbcTypeconv = "io.rdbc" %% "rdbc-typeconv" % Version.rdbc
  val reactiveStreams = "org.reactivestreams" % "reactive-streams" % "1.0.0"
  val scodecBits = "org.scodec" %% "scodec-bits" % "1.1.0"
  val scodecCore = "org.scodec" %% "scodec-core" % "1.10.2"
  val scodecAkka = "org.scodec" %% "scodec-akka" % "0.2.0"
  val typesafeConfig = "com.typesafe" % "config" % "1.3.0"
  val akkaActor = "com.typesafe.akka" %% "akka-actor" % "2.4.10"
  val akkaStream = "com.typesafe.akka" %% "akka-stream" % "2.4.10"
}