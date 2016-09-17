import sbt._

object Library {
  val rdbcCore = "io.rdbc" %% "rdbc-core" % "0.0.2"
  val reactiveStreams = "org.reactivestreams" % "reactive-streams" % "1.0.0"
  val scodecBits = "org.scodec" %% "scodec-bits" % "1.1.0"
  val scodecCore = "org.scodec" %% "scodec-core" % "1.10.2"
  val scodecAkka = "org.scodec" %% "scodec-akka" % "0.2.0"
  val typesafeConfig = "com.typesafe" % "config" % "1.3.0"
  val akkaActor = "com.typesafe.akka" %% "akka-actor" % "2.4.10"
}