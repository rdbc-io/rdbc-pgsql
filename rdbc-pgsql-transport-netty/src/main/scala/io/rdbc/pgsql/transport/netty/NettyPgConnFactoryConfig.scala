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

import java.net.InetSocketAddress

import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.rdbc.ImmutSeq
import io.rdbc.pgsql.core.auth.{Authenticator, UsernamePasswordAuthenticator}
import io.rdbc.pgsql.core.codec.{DecoderFactory, EncoderFactory}
import io.rdbc.pgsql.core.types.PgTypesProvider
import io.rdbc.pgsql.core.{PgConnFactoryConfig, StmtCacheConfig}
import io.rdbc.sapi.{Timeout, TypeConvertersProvider}

import scala.concurrent.ExecutionContext

object NettyPgConnFactoryConfig {

  import PgConnFactoryConfig.{Defaults => PgDefaults}

  object Defaults {
    val channelFactory: ChannelFactory[_ <: Channel] = new NioChannelFactory
    def eventLoopGroup: EventLoopGroup = new NioEventLoopGroup
    val channelOptions: ImmutSeq[ChannelOptionValue[_]] = {
      Vector(ChannelOptionValue(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE))
    }
  }

  def apply(host: String, port: Int, username: String, password: String)
           (dbUser: String = username,
            dbName: String = username,
            subscriberBufferCapacity: Int = PgDefaults.subscriberBufferCapacity,
            subscriberMinDemandRequestSize: Int = PgDefaults.subscriberMinDemandRequestSize,
            stmtCacheConfig: StmtCacheConfig = PgDefaults.stmtCacheConfig,
            typeConvertersProviders: ImmutSeq[TypeConvertersProvider] = PgDefaults.typeConvertersProviders,
            pgTypesProviders: ImmutSeq[PgTypesProvider] = PgDefaults.pgTypesProviders,
            msgDecoderFactory: DecoderFactory = PgDefaults.msgDecoderFactory,
            msgEncoderFactory: EncoderFactory = PgDefaults.msgEncoderFactory,
            writeTimeout: Timeout = PgDefaults.writeTimeout,
            ec: ExecutionContext = PgDefaults.ec,
            channelFactory: ChannelFactory[_ <: Channel] = Defaults.channelFactory,
            eventLoopGroup: EventLoopGroup = Defaults.eventLoopGroup,
            channelOptions: ImmutSeq[ChannelOptionValue[_]] = Defaults.channelOptions
           ): NettyPgConnFactoryConfig = {
    val pgConfig = PgConnFactoryConfig(
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
    NettyPgConnFactoryConfig(
      pgConfig = pgConfig,
      channelFactory = channelFactory,
      eventLoopGroup = eventLoopGroup,
      channelOptions = channelOptions
    )
  }

  def withCustomAuth(host: String, port: Int, authenticator: Authenticator, dbUser: String, dbName: String)
                    (subscriberBufferCapacity: Int = PgDefaults.subscriberBufferCapacity,
                     subscriberMinDemandRequestSize: Int = PgDefaults.subscriberMinDemandRequestSize,
                     stmtCacheConfig: StmtCacheConfig = PgDefaults.stmtCacheConfig,
                     typeConvertersProviders: ImmutSeq[TypeConvertersProvider] = PgDefaults.typeConvertersProviders,
                     pgTypesProviders: ImmutSeq[PgTypesProvider] = PgDefaults.pgTypesProviders,
                     msgDecoderFactory: DecoderFactory = PgDefaults.msgDecoderFactory,
                     msgEncoderFactory: EncoderFactory = PgDefaults.msgEncoderFactory,
                     writeTimeout: Timeout = PgDefaults.writeTimeout,
                     ec: ExecutionContext = PgDefaults.ec,
                     channelFactory: ChannelFactory[_ <: Channel] = Defaults.channelFactory,
                     eventLoopGroup: EventLoopGroup = Defaults.eventLoopGroup,
                     channelOptions: ImmutSeq[ChannelOptionValue[_]] = Defaults.channelOptions
                    ): NettyPgConnFactoryConfig = {
    val pgConfig = PgConnFactoryConfig(
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
    NettyPgConnFactoryConfig(
      pgConfig = pgConfig,
      channelFactory = channelFactory,
      eventLoopGroup = eventLoopGroup,
      channelOptions = channelOptions
    )
  }

  def defaultsWithCustomAuth(host: String,
               port: Int,
               authenticator: Authenticator,
               dbRole: String,
               dbName: String): NettyPgConnFactoryConfig = {
    withCustomAuth(host, port, authenticator, dbRole, dbName)()
  }

  def defaults(host: String, port: Int, username: String, password: String): NettyPgConnFactoryConfig = {
    apply(host, port, username, password)()
  }
}

final case class NettyPgConnFactoryConfig(pgConfig: PgConnFactoryConfig,
                                          channelFactory: ChannelFactory[_ <: Channel],
                                          eventLoopGroup: EventLoopGroup,
                                          channelOptions: ImmutSeq[ChannelOptionValue[_]])
