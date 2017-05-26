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

import io.rdbc.pgsql.core._
import io.rdbc.pgsql.core.exception.PgProtocolViolationException
import io.rdbc.pgsql.core.internal.fsm.{State, StateAction}
import io.rdbc.pgsql.core.internal.{PgRowPublisher, PortalDescData}
import io.rdbc.pgsql.core.pgstruct.messages.backend._
import io.rdbc.pgsql.core.pgstruct.messages.frontend.PortalName

import scala.concurrent.{ExecutionContext, Promise}

private[core]
class StrmWaitingForDescribe private[fsm](txMgmt: Boolean,
                                          publisher: PgRowPublisher,
                                          portalName: Option[PortalName],
                                          describePromise: Promise[PortalDescData],
                                          parsePromise: Promise[Unit])
                                         (implicit out: ChannelWriter, ec: ExecutionContext)
  extends State {

  @volatile private[this] var maybePortalDescData = Option.empty[PortalDescData]

  protected val msgHandler: PgMsgHandler = {
    case ParseComplete =>
      parsePromise.success(())
      stay

    case BindComplete => stay
    case _: ParameterDescription => stay
    case NoData => completeDescribePromise(RowDescription.empty)
    case rowDesc: RowDescription => completeDescribePromise(rowDesc)

    case _: ReadyForQuery =>
      maybePortalDescData match {
        case None =>
          val ex = new PgProtocolViolationException(
            "Ready for query received without prior row description"
          )
          fatal(ex) andThenF sendFailureToClient(ex)

        case Some(afterDescData) =>
          goto(State.Streaming.pullingRows(txMgmt, afterDescData, publisher)) andThenF publisher.resume()
      }
  }

  private def completeDescribePromise(rowDesc: RowDescription): StateAction = {
    val warningsPromise = Promise[Vector[StatusMessage.Notice]]
    val rowsAffectedPromise = Promise[Long]

    val portalDescData = PortalDescData(
      rowDesc = rowDesc,
      warningsPromise = warningsPromise,
      rowsAffectedPromise = rowsAffectedPromise
    )
    maybePortalDescData = Some(portalDescData)

    describePromise.success(portalDescData)
    stay
  }

  private def sendFailureToClient(ex: Throwable): Unit = {
    maybePortalDescData match {
      case Some(PortalDescData(_, warningsPromise, rowsAffectedPromise)) =>
        warningsPromise.failure(ex)
        rowsAffectedPromise.failure(ex)
        parsePromise.failure(ex)
        describePromise.failure(ex)

      case None =>
        if (!parsePromise.isCompleted) {
          parsePromise.failure(ex) //TODO this is not safe, is it? maybe it is because of single I/O thread
        }
        describePromise.failure(ex)
    }
    publisher.failure(ex)
    //TODO will this failure be signalled twice by publisher?
    // one by failure, second by describePromise?
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
