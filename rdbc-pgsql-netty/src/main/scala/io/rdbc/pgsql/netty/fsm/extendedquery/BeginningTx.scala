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

package io.rdbc.pgsql.netty.fsm.extendedquery

import io.rdbc.pgsql.core.SessionParams
import io.rdbc.pgsql.core.messages.backend.{CommandComplete, ReadyForQuery}
import io.rdbc.pgsql.core.messages.frontend._
import io.rdbc.pgsql.core.types.PgTypeRegistry
import io.rdbc.pgsql.netty.{ChannelWriter, PgConnection, PgResultStream, TimeoutScheduler}
import io.rdbc.sapi.TypeConverterRegistry

import scala.concurrent.{ExecutionContext, Future, Promise}

object BeginningTx {
  def apply(parse: Option[Parse], bind: Bind, streamPromise: Promise[PgResultStream], parsePromise: Promise[Unit], sessionParams: SessionParams,
            timeoutScheduler: TimeoutScheduler)
               (implicit out: ChannelWriter, rdbcTypeConvRegistry: TypeConverterRegistry,
                pgTypeConvRegistry: PgTypeRegistry, ec: ExecutionContext): BeginningTx = {
    new BeginningTx(parse, bind, streamPromise, parsePromise, sessionParams, timeoutScheduler)
  }
}

class BeginningTx protected(maybeParse: Option[Parse],
                            bind: Bind,
                            streamPromise: Promise[PgResultStream],
                            parsePromise: Promise[Unit],
                            sessionParams: SessionParams,
                            timeoutScheduler: TimeoutScheduler)
                           (implicit out: ChannelWriter,
                            rdbcTypeConvRegistry: TypeConverterRegistry,
                            pgTypeConvRegistry: PgTypeRegistry, ec: ExecutionContext)
  extends ExtendedQueryingCommon {

  private var beginComplete = false

  def handleMsg = handleCommon.orElse {
    case CommandComplete("BEGIN", _) =>
      beginComplete = true
      stay

    case ReadyForQuery(_) if beginComplete =>
      maybeParse.foreach(out.write(_))
      goto(WaitingForDescribe.withTxMgmt(bind.portal, streamPromise, parsePromise, sessionParams: SessionParams, timeoutScheduler)) andThen {
        out.writeAndFlush(bind, Describe(PortalType, bind.portal), Sync)
      }
  }

  val shortDesc = "extended_querying.beginning_tx"
}
