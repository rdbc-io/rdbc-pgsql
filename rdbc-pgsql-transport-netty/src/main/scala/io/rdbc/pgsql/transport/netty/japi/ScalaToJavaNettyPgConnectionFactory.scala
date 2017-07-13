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

package io.rdbc.pgsql.transport.netty.japi

import io.netty.channel.ChannelOption
import io.rdbc.pgsql.transport.netty.japi.Conversions._
import io.rdbc.pgsql.transport.netty.japi.NettyPgConnectionFactory.Config
import io.rdbc.pgsql.transport.netty.sapi.NettyPgConnectionFactory.{Config => SConfig}
import io.rdbc.pgsql.transport.netty.sapi.{
  ChannelOptionValue => SChannelOptionValue,
  NettyPgConnectionFactory => SNettyPgConnectionFactory
}
import io.rdbc.typeconv.StandardTypeConvertersProvider

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

object ScalaToJavaNettyPgConnectionFactory {

  def create(config: Config): NettyPgConnectionFactory = {

    val sConfig = SConfig.apply(
      host = config.host(),
      port = config.port(),
      authenticator = config.authenticator(),
      dbName = config.dbName().asScala,
      subscriberBufferCapacity = config.subscriberBufferCapacity(),
      subscriberMinDemandRequestSize = config.subscriberMinDemandRequestSize(),
      stmtCacheConfig = config.cacheConfig().asScala,
      typeConvertersProviders = Vector(new StandardTypeConvertersProvider),
      pgTypesProviders = config.pgTypesProviders().asScala.toVector,
      msgDecoderFactory = config.msgDecoderFactory(),
      msgEncoderFactory = config.msgEncoderFactory(),
      writeTimeout = config.writeTimeout().asScala,
      ec = config.executionContext(),
      channelFactory = config.channelFactory(),
      eventLoopGroup = config.eventLoopGroup(),
      channelOptions = config.channelOptions().asScala.map { jopt =>
        SChannelOptionValue[Any](
          jopt.option().asInstanceOf[ChannelOption[Any]],
          jopt.value()
        )
      }.toVector
    )

    val underlying = SNettyPgConnectionFactory.apply(sConfig)

    new NettyPgConnectionFactory(underlying, config.executionContext())
  }

}
