/*
 * Copyright 2016-2017 Krzysztof Pado
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

package io.rdbc.pgsql.core.internal.fsm.streaming

import io.rdbc.pgsql.core.exception.PgProtocolViolationException
import io.rdbc.pgsql.core.internal.fsm.{State, StateAction}
import io.rdbc.pgsql.core.internal.scheduler.TimeoutHandler
import io.rdbc.pgsql.core.internal.{PgResultStream, PgRowPublisher}
import io.rdbc.pgsql.core.pgstruct.messages.backend._
import io.rdbc.pgsql.core.pgstruct.messages.frontend.PortalName
import io.rdbc.pgsql.core.types.PgTypeRegistry
import io.rdbc.pgsql.core.util.concurrent.LockFactory
import io.rdbc.pgsql.core.{ChannelWriter, FatalErrorNotifier, PgMsgHandler, SessionParams}
import io.rdbc.sapi.TypeConverterRegistry

import scala.concurrent.{ExecutionContext, Promise}

private[core]
class StrmWaitingForDescribe private[fsm](txMgmt: Boolean,
                                          portalName: Option[PortalName],
                                          streamPromise: Promise[PgResultStream],
                                          parsePromise: Promise[Unit],
                                          pgTypes: PgTypeRegistry,
                                          typeConverters: TypeConverterRegistry,
                                          sessionParams: SessionParams,
                                          timeoutHandler: TimeoutHandler,
                                          lockFactory: LockFactory,
                                          fatalErrorNotifier: FatalErrorNotifier)
                                         (implicit out: ChannelWriter, ec: ExecutionContext)
  extends State {

  @volatile private[this] var maybeAfterDescData = Option.empty[AfterDescData]

  protected val msgHandler: PgMsgHandler = {
    case ParseComplete =>
      parsePromise.success(())
      stay

    case BindComplete => stay
    case _: ParameterDescription => stay
    case NoData => completeStreamPromise(RowDescription.empty)
    case rowDesc: RowDescription => completeStreamPromise(rowDesc)

    case _: ReadyForQuery =>
      maybeAfterDescData match {
        case None =>
          val ex = new PgProtocolViolationException(
            "Ready for query received without prior row description"
          )
          fatal(ex) andThenF sendFailureToClient(ex)

        case Some(afterDescData@AfterDescData(publisher, _, _)) =>
          goto(new StrmPullingRows(txMgmt, afterDescData)) andThenF publisher.resume()
      }
  }

  private def completeStreamPromise(rowDesc: RowDescription): StateAction = {
    val publisher = createPublisher(rowDesc)
    val warningsPromise = Promise[Vector[StatusMessage.Notice]]
    val rowsAffectedPromise = Promise[Long]

    val afterDescData = AfterDescData(
      publisher = publisher,
      warningsPromise = warningsPromise,
      rowsAffectedPromise = rowsAffectedPromise
    )
    maybeAfterDescData = Some(afterDescData)

    val stream = createStream(afterDescData, rowDesc)
    streamPromise.success(stream)
    stay
  }

  private def createStream(afterDescData: AfterDescData,
                           rowDesc: RowDescription): PgResultStream = {
    new PgResultStream(
      rows = afterDescData.publisher,
      rowDesc = rowDesc,
      rowsAffected = afterDescData.rowsAffectedPromise.future,
      warningMsgsFut = afterDescData.warningsPromise.future,
      pgTypes = pgTypes,
      typeConverters = typeConverters
    )
  }

  private def createPublisher(rowDesc: RowDescription): PgRowPublisher = {
    new PgRowPublisher(
      rowDesc = rowDesc,
      portalName = portalName,
      pgTypes = pgTypes,
      typeConverters = typeConverters,
      sessionParams = sessionParams,
      timeoutHandler = timeoutHandler,
      lockFactory = lockFactory,
      fatalErrorNotifier = fatalErrorNotifier
    )
  }

  private def sendFailureToClient(ex: Throwable): Unit = {
    maybeAfterDescData match {
      case Some(AfterDescData(publisher, warningsPromise, rowsAffectedPromise)) =>
        publisher.failure(ex)
        warningsPromise.failure(ex)
        rowsAffectedPromise.failure(ex)
        parsePromise.failure(ex)

      case None => streamPromise.failure(ex)
    }
  }

  protected def onNonFatalError(ex: Throwable): StateAction = {
    goto(State.Streaming.queryFailed(txMgmt, portalName) {
      sendFailureToClient(ex)
    })
  }

  protected def onFatalError(ex: Throwable): Unit = {
    sendFailureToClient(ex)
  }
}
