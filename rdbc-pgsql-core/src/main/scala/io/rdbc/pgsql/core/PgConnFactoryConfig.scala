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

package io.rdbc.pgsql.core

import java.net.InetSocketAddress

import io.rdbc.ImmutSeq
import io.rdbc.pgsql.core.auth.{Authenticator, UsernamePasswordAuthenticator}
import io.rdbc.pgsql.core.codec.scodec.{ScodecDecoderFactory, ScodecEncoderFactory}
import io.rdbc.pgsql.core.codec.{DecoderFactory, EncoderFactory}
import io.rdbc.pgsql.core.types.PgTypesProvider
import io.rdbc.pgsql.core.types.scodec.ScodecPgTypesProvider
import io.rdbc.sapi.{Timeout, TypeConvertersProvider}
import io.rdbc.typeconv.StandardTypeConvertersProvider

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object PgConnFactoryConfig {
  def apply(host: String,
            port: Int,
            authenticator: Authenticator,
            dbRole: String,
            dbName: String): PgConnFactoryConfig = {
    PgConnFactoryConfig(
      address = InetSocketAddress.createUnresolved(host, port),
      dbRole = dbRole,
      dbName = dbName,
      authenticator = authenticator,
      subscriberBufferCapacity = 100,
      subscriberMinDemandRequestSize = 10,
      stmtCacheConfig = StmtCacheConfig.Enabled(capacity = 100),
      typeConvertersProviders = Vector(new StandardTypeConvertersProvider),
      pgTypesProviders = Vector(new ScodecPgTypesProvider),
      msgDecoderFactory = new ScodecDecoderFactory,
      msgEncoderFactory = new ScodecEncoderFactory,
      writeTimeout = Timeout(10.seconds),
      ec = ExecutionContext.global
    )
  }

  def apply(host: String, port: Int, username: String, password: String): PgConnFactoryConfig = {
    apply(host, port, new UsernamePasswordAuthenticator(username, password),
      dbRole = username, dbName = username
    )
  }
}

final case class PgConnFactoryConfig(address: InetSocketAddress,
                                     dbRole: String,
                                     dbName: String,
                                     authenticator: Authenticator,
                                     typeConvertersProviders: ImmutSeq[TypeConvertersProvider],
                                     pgTypesProviders: ImmutSeq[PgTypesProvider],
                                     subscriberBufferCapacity: Int,
                                     subscriberMinDemandRequestSize: Int,
                                     stmtCacheConfig: StmtCacheConfig,
                                     msgDecoderFactory: DecoderFactory,
                                     msgEncoderFactory: EncoderFactory,
                                     writeTimeout: Timeout,
                                     ec: ExecutionContext) {

  def withTypeConvertersProvider[A](typeConvertersProvider: TypeConvertersProvider): PgConnFactoryConfig = {
    copy(
      typeConvertersProviders = typeConvertersProviders.toVector :+ typeConvertersProvider
    )
  }

  def withPgTypesProvider[A](pgTypesProvider: PgTypesProvider): PgConnFactoryConfig = {
    copy(
      pgTypesProviders = pgTypesProviders.toVector :+ pgTypesProvider
    )
  }

}
