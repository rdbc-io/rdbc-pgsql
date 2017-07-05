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

  object Defaults {
    val subscriberBufferCapacity: Int = 100
    val subscriberMinDemandRequestSize: Int = 10
    val stmtCacheConfig: StmtCacheConfig = StmtCacheConfig.Enabled(capacity = 100)
    val typeConvertersProviders: ImmutSeq[TypeConvertersProvider] = Vector(new StandardTypeConvertersProvider)
    val pgTypesProviders: ImmutSeq[PgTypesProvider] = Vector(new ScodecPgTypesProvider)
    val msgDecoderFactory: DecoderFactory = new ScodecDecoderFactory
    val msgEncoderFactory: EncoderFactory = new ScodecEncoderFactory
    val writeTimeout: Timeout = Timeout(10.seconds)
    val ec: ExecutionContext = ExecutionContext.global
  }

  def apply(host: String, port: Int, username: String, password: String)
           (dbUser: String = username,
            dbName: String = username,
            subscriberBufferCapacity: Int = Defaults.subscriberBufferCapacity,
            subscriberMinDemandRequestSize: Int = Defaults.subscriberMinDemandRequestSize,
            stmtCacheConfig: StmtCacheConfig = Defaults.stmtCacheConfig,
            typeConvertersProviders: ImmutSeq[TypeConvertersProvider] = Defaults.typeConvertersProviders,
            pgTypesProviders: ImmutSeq[PgTypesProvider] = Defaults.pgTypesProviders,
            msgDecoderFactory: DecoderFactory = Defaults.msgDecoderFactory,
            msgEncoderFactory: EncoderFactory = Defaults.msgEncoderFactory,
            writeTimeout: Timeout = Defaults.writeTimeout,
            ec: ExecutionContext = Defaults.ec
           ): PgConnFactoryConfig = {
    PgConnFactoryConfig(
      address = InetSocketAddress.createUnresolved(host, port),
      dbUser = dbUser,
      dbName = dbName,
      authenticator = new UsernamePasswordAuthenticator(username, password),
      typeConvertersProviders = typeConvertersProviders,
      pgTypesProviders = pgTypesProviders,
      subscriberBufferCapacity = subscriberBufferCapacity,
      subscriberMinDemandRequestSize = subscriberMinDemandRequestSize,
      stmtCacheConfig = stmtCacheConfig,
      msgDecoderFactory = msgDecoderFactory,
      msgEncoderFactory = msgEncoderFactory,
      writeTimeout = writeTimeout,
      ec = ec
    )
  }

  def withCustomAuth(host: String, port: Int, authenticator: Authenticator, dbUser: String, dbName: String)
                    (subscriberBufferCapacity: Int = Defaults.subscriberBufferCapacity,
                     subscriberMinDemandRequestSize: Int = Defaults.subscriberMinDemandRequestSize,
                     stmtCacheConfig: StmtCacheConfig = Defaults.stmtCacheConfig,
                     typeConvertersProviders: ImmutSeq[TypeConvertersProvider] = Defaults.typeConvertersProviders,
                     pgTypesProviders: ImmutSeq[PgTypesProvider] = Defaults.pgTypesProviders,
                     msgDecoderFactory: DecoderFactory = Defaults.msgDecoderFactory,
                     msgEncoderFactory: EncoderFactory = Defaults.msgEncoderFactory,
                     writeTimeout: Timeout = Defaults.writeTimeout,
                     ec: ExecutionContext = Defaults.ec
                    ): PgConnFactoryConfig = {
    PgConnFactoryConfig(
      address = InetSocketAddress.createUnresolved(host, port),
      dbUser = dbUser,
      dbName = dbName,
      authenticator = authenticator,
      typeConvertersProviders = typeConvertersProviders,
      pgTypesProviders = pgTypesProviders,
      subscriberBufferCapacity = subscriberBufferCapacity,
      subscriberMinDemandRequestSize = subscriberMinDemandRequestSize,
      stmtCacheConfig = stmtCacheConfig,
      msgDecoderFactory = msgDecoderFactory,
      msgEncoderFactory = msgEncoderFactory,
      writeTimeout = writeTimeout,
      ec = ec
    )
  }

  def defaults(host: String, port: Int, username: String, password: String): PgConnFactoryConfig = {
    apply(host, port, username, password)()
  }

  def defaultsWithCustomAuth(host: String,
                             port: Int,
                             authenticator: Authenticator,
                             dbRole: String,
                             dbName: String): PgConnFactoryConfig = {
    withCustomAuth(host, port, authenticator, dbRole, dbName)()
  }
}

final case class PgConnFactoryConfig(address: InetSocketAddress,
                                     dbUser: String,
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
                                     ec: ExecutionContext)
