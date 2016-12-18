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

package io.rdbc.pgsql.core.fsm.extendedquery

import io.rdbc.pgsql.core.fsm.State
import io.rdbc.pgsql.core.fsm.State.Outcome
import io.rdbc.pgsql.core.messages.backend.{CommandComplete, ReadyForQuery}
import io.rdbc.pgsql.core.messages.frontend._
import io.rdbc.pgsql.core.scheduler.TimeoutScheduler
import io.rdbc.pgsql.core.types.PgTypeRegistry
import io.rdbc.pgsql.core.{ChannelWriter, FatalErrorNotifier, PgResultStream, SessionParams}
import io.rdbc.sapi.TypeConverterRegistry

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

object BeginningTx {
  def apply(parse: Option[Parse], bind: Bind, streamPromise: Promise[PgResultStream], parsePromise: Promise[Unit],
            sessionParams: SessionParams, timeoutScheduler: TimeoutScheduler,
            rdbcTypeConvRegistry: TypeConverterRegistry, pgTypeConvRegistry: PgTypeRegistry,
            fatalErrorNotifier: FatalErrorNotifier)
           (implicit out: ChannelWriter, ec: ExecutionContext): BeginningTx = {
    new BeginningTx(parse, bind, streamPromise, parsePromise, sessionParams, timeoutScheduler, rdbcTypeConvRegistry, pgTypeConvRegistry, fatalErrorNotifier)
  }
}

class BeginningTx protected(maybeParse: Option[Parse],
                            bind: Bind,
                            streamPromise: Promise[PgResultStream],
                            parsePromise: Promise[Unit],
                            sessionParams: SessionParams,
                            timeoutScheduler: TimeoutScheduler,
                            rdbcTypeConvRegistry: TypeConverterRegistry,
                            pgTypeConvRegistry: PgTypeRegistry,
                            fatalErrorNotifier: FatalErrorNotifier)
                           (implicit out: ChannelWriter,
                            ec: ExecutionContext)
  extends State {

  private var beginComplete = false

  def msgHandler = {
    case CommandComplete("BEGIN", _) =>
      beginComplete = true
      stay

    case ReadyForQuery(_) if beginComplete =>
      maybeParse.foreach(out.write(_))
      goto(WaitingForDescribe.withTxMgmt(bind.portal, streamPromise, parsePromise, sessionParams: SessionParams, timeoutScheduler, rdbcTypeConvRegistry, pgTypeConvRegistry, fatalErrorNotifier)) andThen {
        out.writeAndFlush(bind, Describe(PortalType, bind.portal), Sync).recoverWith {
          case NonFatal(ex) =>
            sendFailureToClient(ex)
            Future.failed(ex)
        }
      }
  }

  protected def sendFailureToClient(ex: Throwable): Unit = {
    streamPromise.failure(ex)
    parsePromise.failure(ex)
  }

  protected def onNonFatalError(ex: Throwable): Outcome = {
    goto(Failed(txMgmt = true, bind.portal) {
      sendFailureToClient(ex)
    })
  }

  protected def onFatalError(ex: Throwable): Unit = {
    sendFailureToClient(ex)
  }

  val name = "extended_querying.beginning_tx"

}
