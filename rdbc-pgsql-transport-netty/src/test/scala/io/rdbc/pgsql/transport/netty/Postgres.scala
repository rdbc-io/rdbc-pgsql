/*
 * Copyright 2016 rdbc contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rdbc.pgsql.transport.netty

import java.util.Arrays.asList

import de.flapdoodle.embed.process.distribution.IVersion
import de.flapdoodle.embed.process.io.progress.StandardConsoleProgressListener
import de.flapdoodle.embed.process.runtime.Network
import de.flapdoodle.embed.process.store.PostgresArtifactStoreBuilder
import io.rdbc.pgsql.core.auth.Auth
import io.rdbc.pgsql.transport.netty.NettyPgConnectionFactory.Config
import io.rdbc.sapi.Timeout
import io.rdbc.test._
import ru.yandex.qatools.embed.postgresql.config.{PostgresDownloadConfigBuilder, RuntimeConfigBuilder}
import ru.yandex.qatools.embed.postgresql.distribution.Version
import ru.yandex.qatools.embed.postgresql.distribution.Version.Main.PRODUCTION
import ru.yandex.qatools.embed.postgresql.{Command, EmbeddedPostgres}

class PostgresProcess(
                       val process: EmbeddedPostgres,
                       val connFact: NettyPgConnectionFactory
                     ) {
  def stop()(implicit timeout: Timeout): Unit = {
    process.stop()
    connFact.shutdown().get
  }
}

object Postgres {

  private val pgVersion: IVersion = {
    val verStr = Option(System.getenv("PG_VER")).getOrElse(PRODUCTION.name())
    Version.Main.valueOf(verStr)
  }

  def start(): PostgresProcess = {
    val host = "localhost"
    val port = Network.getFreeServerPort()
    val dbName = "rdbc"
    val user = "rdbc"
    val pass = "rdbc"

    val runtimeCfg = new RuntimeConfigBuilder()
      .defaults(Command.Postgres)
      .artifactStore(
        new PostgresArtifactStoreBuilder()
          .defaults(Command.Postgres)
          .download(
            new PostgresDownloadConfigBuilder()
              .defaultsForCommand(Command.Postgres)
              .progressListener(new StandardConsoleProgressListener() {
                override def progress(label: String, percent: Int): Unit = ()
                override def info(label: String, message: String): Unit = ()
              })
              .build
          )
      ).build

    val postgres = new EmbeddedPostgres(pgVersion)
    postgres.start(runtimeCfg, host, port, dbName, user, pass,
      asList(
        "-E", "UTF-8",
        "--locale=en_US.UTF-8",
        "--lc-collate=en_US.UTF-8",
        "--lc-ctype=en_US.UTF-8"
      )
    )
    new PostgresProcess(postgres, NettyPgConnectionFactory(
      Config(host, port, Auth.password(user, pass))
    ))
  }
}
