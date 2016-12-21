import de.heikoseeberger.sbtheader.license.Apache2_0
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._

lazy val commonSettings = Seq(
  organization := "io.rdbc.pgsql",
  scalaVersion := "2.12.1",
  crossScalaVersions := Vector("2.11.8"),
  scalacOptions ++= Vector(
    "-unchecked",
    "-deprecation",
    "-language:_",
    "-target:jvm-1.8",
    "-encoding", "UTF-8"
  ),
  licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html")),
  bintrayOrganization := Some("rdbc"),
  headers := Map(
    "scala" -> Apache2_0("2016", "Krzysztof Pado")
  ),
  resolvers += Resolver.bintrayRepo("rdbc", "maven"),
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    setNextVersion,
    commitNextVersion,
    pushChanges
  )
)

lazy val rdbcPgsql = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    publishArtifact := false,
    bintrayReleaseOnPublish := false
  )
  .aggregate(core, scodec, nettyTransport)

lazy val core = (project in file("rdbc-pgsql-core"))
  .settings(commonSettings: _*)
  .settings(
    name := "pgsql-core",
    libraryDependencies ++= Vector(
      Library.rdbcScalaApi,
      Library.rdbcTypeconv,
      Library.rdbcImplbase,
      Library.typesafeConfig,
      Library.scalaLogging,
      Library.akkaStream,
      Library.sourcecode
    )
  )

lazy val scodec = (project in file("rdbc-pgsql-scodec"))
  .settings(commonSettings: _*)
  .settings(
    name := "pgsql-codec-scodec",
    libraryDependencies ++= Vector(
      Library.scodecBits,
      Library.scodecCore
    )
  ).dependsOn(core)

lazy val nettyTransport = (project in file("rdbc-pgsql-transport-netty"))
  .settings(commonSettings: _*)
  .settings(
    name := "pgsql-transport-netty",
    libraryDependencies ++= Vector(
      Library.nettyHandler,
      Library.nettyEpoll,
      Library.rdbcTypeconv,
      Library.scalaLogging,
      Library.logback
    )
  ).dependsOn(core, scodec)

lazy val playground = (project in file("rdbc-pgsql-playground"))
  .settings(commonSettings: _*)
  .settings(
    name := "pgsql-playground",
    publishArtifact := false,
    bintrayReleaseOnPublish := false,
    libraryDependencies ++= Vector(
      "org.postgresql" % "postgresql" % "9.4.1211"
    )
  ).dependsOn(core, scodec, nettyTransport)
