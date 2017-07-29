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

import java.sql.{Connection, DriverManager}
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Benchmark)
class JdbcReadBench {

  var connection: Connection = _

  @Setup
  def setup(): Unit = {
    Database.insertTestData()

    connection = DriverManager.getConnection(
      s"jdbc:postgresql://" +
        s"${Database.host}:${Database.port}/${Database.dbName}" +
        s"?user=${Database.user}&password=${Database.password}"
    )
  }

  @TearDown
  def teardown(): Unit = {
    connection.close()
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.SECONDS)
  @BenchmarkMode(Array(Mode.Throughput))
  def bench(blackhole: Blackhole): Unit = {
    val stmt = connection.prepareStatement("select i,v,t from bench")
    val rs = stmt.executeQuery()
    while (rs.next()) {
      blackhole.consume(rs.getInt("i"))
      blackhole.consume(rs.getString("v"))
      blackhole.consume(rs.getTimestamp("t"))
    }
  }

}
