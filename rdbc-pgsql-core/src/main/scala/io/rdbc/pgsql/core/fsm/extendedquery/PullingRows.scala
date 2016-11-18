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

import io.rdbc.pgsql.core.ChannelWriter
import io.rdbc.pgsql.core.fsm.{State, WaitingForReady}
import io.rdbc.pgsql.core.messages.backend._

import scala.concurrent.ExecutionContext

class PullingRows(txMgmt: Boolean, afterDescData: AfterDescData)(implicit out: ChannelWriter, ec: ExecutionContext)
  extends State
    with WarningCollection { //TODO warnings should be collected in all extended query states

  val publisher = afterDescData.publisher
  val warningsPromise = afterDescData.warningsPromise
  val rowsAffectedPromise = afterDescData.rowsAffectedPromise

  def msgHandler = {
    case PortalSuspended =>
      stay

    case dr: DataRow =>
      publisher.handleRow(dr)
      stay

    case ReadyForQuery(_) =>
      publisher.resume()
      stay

    case EmptyQueryResponse =>
      rowsAffectedPromise.success(0L)
      warningsPromise.success(warnings)
      if (txMgmt) goto(new CompletedPendingCommit(publisher))
      else goto(new WaitingForReady(onIdle = publisher.complete(), onFailure = publisher.failure))

    case CommandComplete(_, rowsAffected) =>
      rowsAffectedPromise.success(rowsAffected.map(_.toLong).getOrElse(0L))
      warningsPromise.success(warnings)
      if (txMgmt) goto(new CompletedPendingCommit(publisher))
      else goto(new WaitingForReady(onIdle = publisher.complete(), onFailure = publisher.failure))

    case CloseComplete =>
      if (txMgmt) goto(new CompletedPendingCommit(publisher))
      else goto(new WaitingForReady(onIdle = publisher.complete(), onFailure = publisher.failure))
  }

  protected def onNonFatalError(ex: Throwable) = {
    goto(Failed(txMgmt) {
      sendFailureToClient(ex)
    })
  }

  protected def onFatalError(ex: Throwable) = {
    sendFailureToClient(ex)
  }

  def sendFailureToClient(ex: Throwable) = {
    publisher.failure(ex)
    warningsPromise.failure(ex)
    rowsAffectedPromise.failure(ex)
  }

  val name = "extended_querying.pulling_rows"
}
