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

package io.rdbc.pgsql.core.internal.fsm.streaming

import io.rdbc.pgsql.core.ChannelWriter
import io.rdbc.pgsql.core.internal.fsm.{State, StateAction, WaitingForReady, WarningCollection}
import io.rdbc.pgsql.core.internal.protocol.messages.backend._
import io.rdbc.pgsql.core.internal.protocol.messages.frontend.ColName
import io.rdbc.pgsql.core.internal.{PgMsgHandler, PgRowPublisher, PortalDescData}

import scala.concurrent.ExecutionContext

private[core]
class StrmPullingRows private[fsm](txMgmt: Boolean, afterDescData: PortalDescData, publisher: PgRowPublisher)
                                  (implicit out: ChannelWriter, ec: ExecutionContext)
  extends State
    with WarningCollection {
  //TODO warnings should be collected in all extended query states

  private[this] val nameIdxMapping: Map[ColName, Int] = {
    Map(afterDescData.rowDesc.colDescs.zipWithIndex.map {
      case (cdesc, idx) => cdesc.name -> idx
    }: _*)
  }

  publisher.fatalErrNotifier = (msg, ex) => {
    logger.error(s"Fatal error occured in the publisher: $msg")
    onFatalError(ex)
  }

  private[this] val warningsPromise = afterDescData.warningsPromise
  private[this] val rowsAffectedPromise = afterDescData.rowsAffectedPromise

  val msgHandler: PgMsgHandler = {
    case PortalSuspended => stay

    case dr: DataRow =>
      publisher.handleRow(dr, afterDescData.rowDesc, nameIdxMapping)
      stay

    case ReadyForQuery(_) =>
      publisher.resume()
      stay

    case EmptyQueryResponse =>
      completePulling(0L)

    case CommandComplete(_, rowsAffected) =>
      completePulling(rowsAffected.map(_.toLong).getOrElse(0L))

    //TODO distinguish between cancelled subscription and not-cancelled
    // if the subscription was cancelled no publisher.complete or publisher.failure
    // should happen
    case CloseComplete => //TODO we use only unnamed portals, closing them is not necessary
      if (txMgmt) goto(new StrmPendingCommit(publisher))
      else goto(new WaitingForReady(
        onIdle = publisher.complete(),
        onFailure = publisher.failure)
      )
  }

  private def completePulling(rowsAffected: Long): StateAction.Goto = {
    rowsAffectedPromise.success(rowsAffected)
    warningsPromise.success(warnings)
    if (txMgmt) goto(new StrmPendingCommit(publisher))
    else goto(new StrmPendingClosePortal(publisher, onIdle = publisher.complete()))
  }


  private def sendFailureToClient(ex: Throwable): Unit = {
    publisher.failure(ex)
    warningsPromise.failure(ex)
    rowsAffectedPromise.failure(ex)
  }

  protected def onNonFatalError(ex: Throwable): StateAction = {
    goto(State.Streaming.queryFailed(txMgmt, publisher.portalName) {
      sendFailureToClient(ex)
    })
  }

  protected def onFatalError(ex: Throwable): Unit = {
    sendFailureToClient(ex)
  }
}
