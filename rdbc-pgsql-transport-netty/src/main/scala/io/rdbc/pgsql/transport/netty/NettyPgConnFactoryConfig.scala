/*
 * Copyright 2016 Krzysztof Pado
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

import akka.stream.ActorMaterializerSettings
import com.typesafe.config.{Config, ConfigFactory}
import io.netty.channel._
import io.netty.channel.epoll.{Epoll, EpollEventLoopGroup, EpollSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.rdbc.ImmutSeq
import io.rdbc.pgsql.core.auth.{Authenticator, UsernamePasswordAuthenticator}
import io.rdbc.pgsql.core.codec.{DecoderFactory, EncoderFactory}
import io.rdbc.pgsql.core.types.PgTypesProvider
import io.rdbc.pgsql.core.util.concurrent.{BlockLockFactory, LockFactory}
import io.rdbc.pgsql.scodec.types.ScodecPgTypesProvider
import io.rdbc.pgsql.scodec.{ScodecDecoderFactory, ScodecEncoderFactory}
import io.rdbc.sapi.TypeConvertersProvider
import io.rdbc.typeconv.StandardTypeConvertersProvider

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object NettyPgConnFactoryConfig {

  def apply(username: String, password: String): NettyPgConnFactoryConfig = {
    apply(new UsernamePasswordAuthenticator(username, password), username, username)
  }

  def apply(host: String, port: Int, username: String, password: String): NettyPgConnFactoryConfig = {
    apply(username, password)
      .withHost(host)
      .withPort(port)
  }

  def apply(authenticator: Authenticator, dbRole: String, dbName: String): NettyPgConnFactoryConfig = {
    val tconfig = ConfigFactory.load()

    NettyPgConnFactoryConfig(
      address = InetSocketAddress.createUnresolved("localhost", 5432),
      dbRole = dbRole,
      dbName = dbName,
      authenticator = authenticator,
      maxBatchSize = 100L,
      stmtCacheCapacity = 100,
      typeConvertersProviders = Vector(new StandardTypeConvertersProvider),
      pgTypesProviders = Vector(new ScodecPgTypesProvider),
      msgDecoderFactory = new ScodecDecoderFactory,
      msgEncoderFactory = new ScodecEncoderFactory,
      writeTimeout = 30.seconds,
      channelFactory = defaultChannelFactory,
      eventLoopGroup = defaultEventLoopGroup,
      channelOptions = Vector(ChannelOptionValue(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE)),
      actorSystemConfig = tconfig,
      actorMaterializerSettings = ActorMaterializerSettings(tconfig.getConfig("akka.stream.materializer")),
      lockFactory = new BlockLockFactory,
      ec = ExecutionContext.global
    )
  }

  private def defaultChannelFactory: ChannelFactory[SocketChannel] = {
    if (Epoll.isAvailable) new ReflectiveChannelFactory(classOf[EpollSocketChannel])
    else new ReflectiveChannelFactory(classOf[NioSocketChannel])
  }

  private def defaultEventLoopGroup: EventLoopGroup = {
    if (Epoll.isAvailable) new EpollEventLoopGroup()
    else new NioEventLoopGroup()
  }
}

case class NettyPgConnFactoryConfig(address: InetSocketAddress,
                                    dbRole: String,
                                    dbName: String,
                                    authenticator: Authenticator,
                                    maxBatchSize: Long,
                                    stmtCacheCapacity: Int,
                                    typeConvertersProviders: ImmutSeq[TypeConvertersProvider],
                                    pgTypesProviders: ImmutSeq[PgTypesProvider],
                                    msgDecoderFactory: DecoderFactory,
                                    msgEncoderFactory: EncoderFactory,
                                    writeTimeout: FiniteDuration,
                                    channelFactory: ChannelFactory[_ <: Channel],
                                    eventLoopGroup: EventLoopGroup,
                                    channelOptions: ImmutSeq[ChannelOptionValue[_]],
                                    actorSystemConfig: Config,
                                    actorMaterializerSettings: ActorMaterializerSettings,
                                    lockFactory: LockFactory,
                                    ec: ExecutionContext) {

  def withHost(host: String): NettyPgConnFactoryConfig = {
    copy(
      address = InetSocketAddress.createUnresolved(host, address.getPort)
    )
  }

  def withPort(port: Int): NettyPgConnFactoryConfig = {
    copy(
      address = InetSocketAddress.createUnresolved(address.getHostString, port)
    )
  }

  def withUserPassAuth(user: String, password: String): NettyPgConnFactoryConfig = {
    copy(
      authenticator = new UsernamePasswordAuthenticator(user, password),
      dbRole = user,
      dbName = user
    )
  }

  def withChannelOption[A](channelOption: ChannelOption[A], value: A): NettyPgConnFactoryConfig = {
    copy(
      channelOptions = channelOptions.toVector :+ ChannelOptionValue(channelOption, value)
    )
  }

  def withTypeConvertersProvider[A](typeConvertersProvider: TypeConvertersProvider): NettyPgConnFactoryConfig = {
    copy(
      typeConvertersProviders = typeConvertersProviders.toVector :+ typeConvertersProvider
    )
  }

  def withPgTypesProvider[A](pgTypesProvider: PgTypesProvider): NettyPgConnFactoryConfig = {
    copy(
      pgTypesProviders = pgTypesProviders.toVector :+ pgTypesProvider
    )
  }

}
