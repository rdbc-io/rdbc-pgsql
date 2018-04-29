import Settings._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._

import scala.Console._

shellPrompt.in(ThisBuild) := (state => s"${CYAN}project:$GREEN${Project.extract(state).currentRef.project}$RESET> ")

lazy val commonSettings = Vector(
  organization := "io.rdbc.pgsql",
  organizationName := "rdbc contributors",
  scalaVersion := "2.12.4",
  crossScalaVersions := Vector(scalaVersion.value, "2.11.12"),

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
  .aggregate(core, nettyTransport, coreJava, bench)

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
      Library.scalaLogging,
      Library.sourcecode,
      Library.scodecCore,
      Library.scodecBits,
      Library.stm,
      Library.logback % Test,
      Library.scalatest % Test,
      Library.reactiveStreamsTck % Test
    ),
    buildInfoPackage := "io.rdbc.pgsql.core"
  )

lazy val coreJava = (project in file("rdbc-pgsql-core-java"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings: _*)
  .settings(
    name := "pgsql-core-java",
    libraryDependencies ++= Vector(
      Library.immutables % Provided
    ),
    buildInfoPackage := "io.rdbc.pgsql.core.japi"
  ).dependsOn(core)

lazy val nettyTransport = (project in file("rdbc-pgsql-transport-netty"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings: _*)
  .settings(
    name := "pgsql-transport-netty",
    libraryDependencies ++= Vector(
      Library.rdbcJavaApi,
      Library.nettyHandler,
      Library.rdbcTypeconv,
      Library.rdbcUtil,
      Library.rdbcJavaAdapter,
      Library.scalaLogging,
      Library.immutables % Provided,
      Library.logback % Test,
      Library.rdbcTck % Test,
      Library.scalatest % Test,
      Library.pgsql % Test
    ),
    buildInfoPackage := "io.rdbc.pgsql.transport.netty"
  ).dependsOn(core, coreJava)

lazy val doc = (project in file("rdbc-pgsql-doc"))
  .enablePlugins(TemplateReplace)
  .settings(
    publishArtifact := false,
    mkdocsVariables := Map(
      "version" -> version.value,
      "rdbc_version" -> Library.rdbcScalaApi.revision
    )
  )

lazy val bench = (project in file("rdbc-pgsql-bench"))
  .enablePlugins(JmhPlugin)
  .settings(commonSettings: _*)
  .settings(
    name := "pgsql-bench",
    publishArtifact := false,
    libraryDependencies ++= Vector(
      Library.pgsqljdbc
    )
  ).dependsOn(core, nettyTransport)
