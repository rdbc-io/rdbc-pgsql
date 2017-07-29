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

import java.util.concurrent.TimeUnit

import io.rdbc.pgsql.core.config.sapi.Auth
import io.rdbc.pgsql.transport.netty.sapi.NettyPgConnectionFactory
import io.rdbc.sapi._
import org.openjdk.jmh.annotations.{Timeout => _, _}
import org.openjdk.jmh.infra.Blackhole

import scala.concurrent.Await
import scala.concurrent.duration.Duration

@State(Scope.Benchmark)
class ReadBench {

  var connection: Connection = _
  var connFact: ConnectionFactory = _

  implicit val timeout = Timeout.Inf

  @Setup
  def setup(): Unit = {
    Database.insertTestData()

    connFact = NettyPgConnectionFactory(NettyPgConnectionFactory.Config(
      host = Database.host,
      port = Database.port,
      authenticator = Auth.password(Database.user, Database.password),
      dbName = Some(Database.dbName)
    ))
    connection = Await.result(connFact.connection(), Duration.Inf)
  }

  @TearDown
  def teardown(): Unit = {
    Await.result(connection.forceRelease(), Duration.Inf)
    Await.result(connFact.shutdown(), Duration.Inf)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def bench(blackhole: Blackhole): Unit = {
    val rs = Await.result(connection.statement(sql"select i,v,t from bench").executeForSet(), Duration.Inf)
    val iter = rs.rows.iterator
    while (iter.hasNext) {
      val row = iter.next()
      blackhole.consume(row.int("i"))
      blackhole.consume(row.str("v"))
      blackhole.consume(row.instant("t"))
    }
  }
}
