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

import java.util.concurrent.TimeUnit

import io.rdbc.pgsql.core.config.japi.StmtCacheConfig
import io.rdbc.pgsql.core.config.sapi.{StmtCacheConfig => SStmtCacheConfig}
import io.rdbc.sapi.Timeout

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

private[japi] object Conversions {

  implicit class JavaDurationToTimeout(val value: java.time.Duration) extends AnyVal {
    def asScala: Timeout = {
      throwOnFailure {
        Try(value.toNanos).map { nanos =>
          Timeout(FiniteDuration(nanos, TimeUnit.NANOSECONDS))
        }.recover {
          case _: ArithmeticException => Timeout.Inf
        }
      }
    }
  }

  implicit class JavaCacheConfigToScala(val value: StmtCacheConfig) extends AnyVal {
    def asScala: SStmtCacheConfig = {
      if (value.enabled()) {
        SStmtCacheConfig.Enabled(value.capacity())
      } else {
        SStmtCacheConfig.Disabled
      }
    }
  }

}
