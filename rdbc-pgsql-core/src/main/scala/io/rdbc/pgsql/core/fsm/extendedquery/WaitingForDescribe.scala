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

import io.rdbc.pgsql.core.exception.PgProtocolViolationException
import io.rdbc.pgsql.core.fsm.State
import io.rdbc.pgsql.core.fsm.State.Outcome
import io.rdbc.pgsql.core.messages.backend._
import io.rdbc.pgsql.core.scheduler.TimeoutScheduler
import io.rdbc.pgsql.core.types.PgTypeRegistry
import io.rdbc.pgsql.core.{ChannelWriter, FatalErrorNotifier, PgResultStream, PgRowPublisher, SessionParams}
import io.rdbc.sapi.TypeConverterRegistry

import scala.concurrent.{ExecutionContext, Promise}

object WaitingForDescribe {
  def withTxMgmt(portalName: Option[String],
                 streamPromise: Promise[PgResultStream],
                 parsePromise: Promise[Unit],
                 sessionParams: SessionParams,
                 timeoutScheduler: TimeoutScheduler,
                 rdbcTypeConvRegistry: TypeConverterRegistry,
                 pgTypeConvRegistry: PgTypeRegistry,
                 fatalErrorNotifier: FatalErrorNotifier)
                (implicit out: ChannelWriter,
                 ec: ExecutionContext): WaitingForDescribe = {
    new WaitingForDescribe(txMgmt = true, portalName, streamPromise, parsePromise, pgTypeConvRegistry, rdbcTypeConvRegistry, sessionParams, timeoutScheduler, fatalErrorNotifier)
  }

  def withoutTxMgmt(portalName: Option[String],
                    streamPromise: Promise[PgResultStream],
                    parsePromise: Promise[Unit],
                    sessionParams: SessionParams,
                    timeoutScheduler: TimeoutScheduler,
                    rdbcTypeConvRegistry: TypeConverterRegistry,
                    pgTypeConvRegistry: PgTypeRegistry,
                    fatalErrorNotifier: FatalErrorNotifier)
                   (implicit out: ChannelWriter,
                    ec: ExecutionContext): WaitingForDescribe = {
    new WaitingForDescribe(txMgmt = false, portalName, streamPromise, parsePromise, pgTypeConvRegistry, rdbcTypeConvRegistry, sessionParams, timeoutScheduler, fatalErrorNotifier)
  }
}

class WaitingForDescribe protected(txMgmt: Boolean,
                                   portalName: Option[String],
                                   streamPromise: Promise[PgResultStream],
                                   parsePromise: Promise[Unit],
                                   pgTypeConvRegistry: PgTypeRegistry,
                                   rdbcTypeConvRegistry: TypeConverterRegistry,
                                   sessionParams: SessionParams,
                                   timeoutScheduler: TimeoutScheduler,
                                   fatalErrorNotifier: FatalErrorNotifier
                                  )(implicit out: ChannelWriter, ec: ExecutionContext)
  extends State {

  private var maybeAfterDescData = Option.empty[AfterDescData]

  def msgHandler = {
    case ParseComplete =>
      parsePromise.success(())
      stay

    case BindComplete => stay
    case _: ParameterDescription => stay
    case NoData => onRowDescription(RowDescription.empty)
    case rowDesc: RowDescription => onRowDescription(rowDesc)

    case _: ReadyForQuery => maybeAfterDescData match {
      case None => onNonFatalError(new PgProtocolViolationException("ready for query received without prior row desc"))
      case Some(afterDescData@AfterDescData(publisher, _, _)) =>
        goto(new PullingRows(txMgmt, afterDescData)) andThenF {
          publisher.resume()
        }
    }
  }

  private def onRowDescription(rowDesc: RowDescription): Outcome = maybeAfterDescData match {
    case Some(_) => onNonFatalError(new PgProtocolViolationException("already received row description"))
    case None =>
      val publisher = new PgRowPublisher(rowDesc, portalName, pgTypeConvRegistry, rdbcTypeConvRegistry, sessionParams, timeoutScheduler, fatalErrorNotifier)
      val warningsPromise = Promise[Vector[StatusMessage.Notice]]
      val rowsAffectedPromise = Promise[Long]

      maybeAfterDescData = Some(AfterDescData(
        publisher = publisher,
        warningsPromise = warningsPromise,
        rowsAffectedPromise = rowsAffectedPromise
      ))

      val stream = new PgResultStream(
        publisher,
        rowDesc = rowDesc,
        rowsAffected = rowsAffectedPromise.future,
        warningMsgsFut = warningsPromise.future,
        pgTypeConvRegistry = pgTypeConvRegistry,
        rdbcTypeConvRegistry = rdbcTypeConvRegistry
      )
      streamPromise.success(stream)
      stay
  }

  def sendFailureToClient(ex: Throwable): Unit = {
    maybeAfterDescData match {
      case Some(AfterDescData(publisher, warningsPromise, rowsAffectedPromise)) =>
        publisher.failure(ex)
        warningsPromise.failure(ex)
        rowsAffectedPromise.failure(ex)
        parsePromise.failure(ex)

      case None =>
        streamPromise.failure(ex)
    }
  }

  protected def onNonFatalError(ex: Throwable): Outcome = {
    goto(Failed(txMgmt, portalName) {
      sendFailureToClient(ex)
    })
  }

  protected def onFatalError(ex: Throwable): Unit = {
    sendFailureToClient(ex)
  }

  val name = "extended_querying.waiting_for_describe"

}
