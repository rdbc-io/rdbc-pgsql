lazy val commonSettings = Seq(
  organization := "io.rdbc.pgsql",
  version := "0.0.1",
  scalaVersion := "2.11.8",
  scalacOptions ++= Vector(
    "-unchecked",
    "-deprecation",
    "-language:_",
    "-target:jvm-1.8",
    "-encoding", "UTF-8"
  ),
  licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html")),
  bintrayOrganization := Some("rdbc")
)

lazy val rdbcPgsql = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    publishArtifact := false,
    bintrayReleaseOnPublish := false
  )
  .aggregate(core, codec, codecScodec, akkaBackend)

lazy val core = (project in file("rdbc-pgsql-core"))
  .settings(commonSettings: _*)
  .settings(
    name := "pgsql-core",
    libraryDependencies ++= Vector(
      Library.rdbcCore,
      Library.scodecBits,
      Library.scodecCore,
      Library.typesafeConfig
    )
  )

lazy val codec = (project in file("rdbc-pgsql-codec"))
  .settings(commonSettings: _*)
  .settings(
    name := "pgsql-codec",
    libraryDependencies ++= Vector(
      Library.scodecBits
    )
  ).dependsOn(core)

lazy val codecScodec = (project in file("rdbc-pgsql-codec-scodec"))
  .settings(commonSettings: _*)
  .settings(
    name := "pgsql-codec-scodec",
    libraryDependencies ++= Vector(
      Library.scodecBits,
      Library.scodecCore
    )
  ).dependsOn(codec)

lazy val akkaBackend = (project in file("rdbc-pgsql-akka"))
  .settings(commonSettings: _*)
  .settings(
    name := "pgsql-akka-backend",
    libraryDependencies ++= Vector(
      Library.akkaActor,
      Library.scodecBits,
      Library.scodecAkka,
      Library.typesafeConfig
    )
  ).dependsOn(core, codec, codecScodec)

lazy val playground = (project in file("rdbc-pgsql-playground"))
  .settings(commonSettings: _*)
  .settings(
    name := "pgsql-playground",
    publishArtifact := false,
    bintrayReleaseOnPublish := false
  ).dependsOn(core, codec, codecScodec, akkaBackend)
