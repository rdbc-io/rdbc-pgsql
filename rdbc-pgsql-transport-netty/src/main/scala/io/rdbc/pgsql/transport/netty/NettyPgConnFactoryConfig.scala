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

import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.rdbc.ImmutSeq
import io.rdbc.pgsql.core.PgConnFactoryConfig
import io.rdbc.pgsql.core.auth.Authenticator
import io.rdbc.pgsql.core.types.PgTypesProvider
import io.rdbc.sapi.TypeConvertersProvider

object NettyPgConnFactoryConfig {

  def apply(pgConfig: PgConnFactoryConfig): NettyPgConnFactoryConfig = {
    NettyPgConnFactoryConfig(
      pgConfig = pgConfig,
      channelFactory = defaultChannelFactory,
      eventLoopGroup = defaultEventLoopGroup,
      channelOptions = Vector(ChannelOptionValue(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE))
    )
  }

  def apply(host: String,
            port: Int,
            authenticator: Authenticator,
            dbRole: String,
            dbName: String): NettyPgConnFactoryConfig = {
    apply(PgConnFactoryConfig(host, port, authenticator, dbRole, dbName))
  }

  def apply(host: String, port: Int, username: String, password: String): NettyPgConnFactoryConfig = {
    apply(PgConnFactoryConfig(host, port, username, password))
  }

  private def defaultChannelFactory: ChannelFactory[_ <: SocketChannel] = {
    new NioChannelFactory
  }

  private def defaultEventLoopGroup: EventLoopGroup = {
    new NioEventLoopGroup()
  }
}

final case class NettyPgConnFactoryConfig(pgConfig: PgConnFactoryConfig,
                                    channelFactory: ChannelFactory[_ <: Channel],
                                    eventLoopGroup: EventLoopGroup,
                                    channelOptions: ImmutSeq[ChannelOptionValue[_]]) {

  def withTypeConvertersProvider[A](typeConvertersProvider: TypeConvertersProvider): NettyPgConnFactoryConfig = {
    copy(
      pgConfig = pgConfig.withTypeConvertersProvider(typeConvertersProvider)
    )
  }

  def withPgTypesProvider[A](pgTypesProvider: PgTypesProvider): NettyPgConnFactoryConfig = {
    copy(
      pgConfig = pgConfig.withPgTypesProvider(pgTypesProvider)
    )
  }

  def withChannelOption[A](channelOption: ChannelOption[A], value: A): NettyPgConnFactoryConfig = {
    copy(
      channelOptions = channelOptions.toVector :+ ChannelOptionValue(channelOption, value)
    )
  }

}
