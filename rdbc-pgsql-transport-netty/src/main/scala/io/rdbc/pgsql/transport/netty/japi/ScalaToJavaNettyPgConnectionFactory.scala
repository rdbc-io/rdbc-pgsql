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

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

object ScalaToJavaNettyPgConnectionFactory {

  def create(config: Config): NettyPgConnectionFactory = {

    val sConfig = SConfig.apply(
      host = config.getHost(),
      port = config.getPort(),
      authenticator = config.getAuthenticator(),
      dbName = config.getDbName().asScala,
      subscriberBufferCapacity = config.getSubscriberBufferCapacity(),
      subscriberMinDemandRequestSize = config.getSubscriberMinDemandRequestSize(),
      stmtCacheConfig = config.getCacheConfig().asScala,
      writeTimeout = config.getWriteTimeout().asScala,
      ec = config.getExecutionContext(),
      channelFactory = config.getChannelFactory(),
      eventLoopGroup = config.getEventLoopGroup(), //TODO new converters
      channelOptions = config.getChannelOptions().asScala.map { jopt =>
        SChannelOptionValue[Any](
          jopt.getOption().asInstanceOf[ChannelOption[Any]],
          jopt.getValue()
        )
      }.toVector
    )

    val underlying = SNettyPgConnectionFactory.apply(sConfig)

    new NettyPgConnectionFactory(underlying, config.getExecutionContext())
  }

}
