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

import io.rdbc.ImmutSeq
import io.rdbc.pgsql.core.PgConnFactoryConfig.Defaults
import io.rdbc.pgsql.core.auth.Authenticator
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
}

final case class PgConnFactoryConfig
(host: String,
 port: Int,
 authenticator: Authenticator,
 dbName: Option[String] = None,
 subscriberBufferCapacity: Int = Defaults.subscriberBufferCapacity,
 subscriberMinDemandRequestSize: Int = Defaults.subscriberMinDemandRequestSize,
 stmtCacheConfig: StmtCacheConfig = Defaults.stmtCacheConfig,
 typeConvertersProviders: ImmutSeq[TypeConvertersProvider] = Defaults.typeConvertersProviders,
 pgTypesProviders: ImmutSeq[PgTypesProvider] = Defaults.pgTypesProviders,
 msgDecoderFactory: DecoderFactory = Defaults.msgDecoderFactory,
 msgEncoderFactory: EncoderFactory = Defaults.msgEncoderFactory,
 writeTimeout: Timeout = Defaults.writeTimeout,
 ec: ExecutionContext = Defaults.ec)
