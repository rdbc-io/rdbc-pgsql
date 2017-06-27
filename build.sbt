import Settings._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._

import scala.Console._

shellPrompt.in(ThisBuild) := (state => s"${CYAN}project:$GREEN${Project.extract(state).currentRef.project}$RESET> ")

lazy val commonSettings = Vector(
  organization := "io.rdbc.pgsql",
  organizationName := "rdbc contributors",
  scalaVersion := "2.12.2",
  crossScalaVersions := Vector("2.11.11"),

  licenses := Vector(
    "Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.html")
  ),
  startYear := Some(Copyright.startYear),

  homepage := Some(url("https://github.com/rdbc-io/rdbc-pgsql")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/rdbc-io/rdbc-pgsql"),
      "scm:git@github.com:rdbc-io/rdbc-pgsql.git"
    )
  ),

  buildInfoKeys := Vector(version, scalaVersion, git.gitHeadCommit, BuildInfoKey.action("buildTime") {
    java.time.Instant.now()
  }),

  scalastyleFailOnError := true
) ++ compilationConf ++ scaladocConf ++ developersConf ++ publishConf ++ testConf

lazy val rdbcPgsql = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    publishArtifact := false
  )
  .aggregate(core, scodec, nettyTransport)

lazy val core = (project in file("rdbc-pgsql-core"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings: _*)
  .settings(
    name := "pgsql-core",
    libraryDependencies ++= Vector(
      Library.rdbcScalaApi,
      Library.rdbcTypeconv,
      Library.rdbcImplbase,
      Library.rdbcUtil,
      Library.typesafeConfig,
      Library.scalaLogging,
      Library.sourcecode,
      Library.scodecBits,
      Library.stm,
      Library.logback % Test,
      Library.scalatest % Test,
      Library.reactiveStreamsTck % Test
    ),
    buildInfoPackage := "io.rdbc.pgsql.core"
  )

lazy val scodec = (project in file("rdbc-pgsql-codec-scodec"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings: _*)
  .settings(
    name := "pgsql-codec-scodec",
    libraryDependencies ++= Vector(
      Library.scodecBits,
      Library.scodecCore
    ),
    buildInfoPackage := "io.rdbc.pgsql.scodec"
  )
  .dependsOn(core)

lazy val nettyTransport = (project in file("rdbc-pgsql-transport-netty"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings: _*)
  .settings(
    name := "pgsql-transport-netty",
    libraryDependencies ++= Vector(
      Library.nettyHandler,
      Library.rdbcTypeconv,
      Library.rdbcUtil,
      Library.scalaLogging,
      Library.logback % Test,
      Library.rdbcTests % Test,
      Library.scalatest % Test,
      Library.pgsql % Test
    ),
    buildInfoPackage := "io.rdbc.pgsql.transport.netty"
  ).dependsOn(core, scodec)

lazy val rdbcPgsqlDoc = (project in file("rdbc-pgsql-doc"))
  .enablePlugins(TemplateReplace)
  .settings(
    publishArtifact := false,
    mkdocsVariables := Map(
      "version" -> version.value
    )
  )
