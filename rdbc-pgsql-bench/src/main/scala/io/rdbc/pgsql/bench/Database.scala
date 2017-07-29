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

package io.rdbc.pgsql.bench

import java.time.{LocalDateTime, ZoneOffset}

import io.rdbc.pgsql.core.config.sapi.Auth
import io.rdbc.pgsql.transport.netty.sapi.NettyPgConnectionFactory
import io.rdbc.sapi._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object Database {

  val host = "localhost"
  val port = 5432
  val user = "postgres"
  val password = "postgres"
  var dbName = "rdbc"

  implicit private val timeout = Timeout.Inf

  def insertTestData(): Unit = {
    val connFact = NettyPgConnectionFactory(NettyPgConnectionFactory.Config(
      host = host,
      port = port,
      authenticator = Auth.password(user, password),
      dbName = Some(dbName)
    ))

    Await.result(
      connFact.withTransaction { conn =>
        var i = 0
        var v = "000"
        var t = LocalDateTime.of(2000, 1, 1, 1, 1, 1).toInstant(ZoneOffset.UTC)

        Await.ready(
          conn.statement(sql"drop table if exists bench").execute(),
          Duration.Inf
        )

        Await.ready(
          conn.statement(sql"create table bench (i int4, v varchar, t timestamptz)").execute(),
          Duration.Inf
        )

        var cnt = 0
        while (cnt < 100) {
          Await.ready(
            conn.statement(sql"insert into bench(i, v, t) values ($i, $v, $t)").execute(),
            Duration.Inf
          )
          i += 1
          v = i.toString.padTo(3, '0')
          t = t.plusSeconds(3600)
          cnt += 1
        }
        Future.successful(())
      }, Duration.Inf
    )
    Await.ready(connFact.shutdown(), Duration.Inf)
  }

}
