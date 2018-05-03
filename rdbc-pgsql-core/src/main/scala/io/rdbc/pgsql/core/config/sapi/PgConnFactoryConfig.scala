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

package io.rdbc.pgsql.core.config.sapi

import io.rdbc.pgsql.core.auth.Authenticator
import io.rdbc.pgsql.core.config.sapi.PgConnFactoryConfig.Defaults
import io.rdbc.pgsql.core.typeconv.{BuiltInTypeConverters, BuiltInTypeMappings, PartialTypeConverter, TypeMapping}
import io.rdbc.pgsql.core.types.{BuiltInCodecs, PgType, PgVal, PgValCodec}
import io.rdbc.sapi.Timeout

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object PgConnFactoryConfig {
  object Defaults {
    val dbName: Option[String] = None
    val subscriberBufferCapacity: Int = 100
    val subscriberMinDemandRequestSize: Int = 10
    val stmtCacheConfig: StmtCacheConfig = StmtCacheConfig.Enabled(capacity = 100)
    val writeTimeout: Timeout = Timeout(10.seconds)
    val typeConverters: Vector[PartialTypeConverter[_]] = BuiltInTypeConverters
    val typeMappings: Vector[TypeMapping[_, _ <: PgType[_ <: PgVal[_]]]] = BuiltInTypeMappings
    val typeCodecs: Vector[PgValCodec[_ <: PgVal[_]]] = BuiltInCodecs
    val ec: ExecutionContext = ExecutionContext.global
  }
}

final case class PgConnFactoryConfig
(host: String,
 port: Int,
 authenticator: Authenticator,
 dbName: Option[String] = Defaults.dbName,
 subscriberBufferCapacity: Int = Defaults.subscriberBufferCapacity,
 subscriberMinDemandRequestSize: Int = Defaults.subscriberMinDemandRequestSize,
 stmtCacheConfig: StmtCacheConfig = Defaults.stmtCacheConfig,
 writeTimeout: Timeout = Defaults.writeTimeout,
 typeConverters: Vector[PartialTypeConverter[_]],
 typeMappings: Vector[TypeMapping[_, _ <: PgType[_ <: PgVal[_]]]],
 typeCodecs: Vector[PgValCodec[_ <: PgVal[_]]],
 ec: ExecutionContext = Defaults.ec)
