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

import io.rdbc.api.exceptions.ConnectionClosedException
import io.rdbc.pgsql.core.ChannelWriter
import io.rdbc.pgsql.core.exception.PgStmtExecutionException
import io.rdbc.pgsql.core.fsm.ConnectionClosed
import io.rdbc.pgsql.core.messages.backend._

import scala.concurrent.ExecutionContext

class PullingRows(txMgmt: Boolean, afterDescData: AfterDescData)(implicit out: ChannelWriter, ec: ExecutionContext) extends ExtendedQueryingCommon {

  val publisher = afterDescData.publisher
  val warningsPromise = afterDescData.warningsPromise
  val rowsAffectedPromise = afterDescData.rowsAffectedPromise

  def handleMsg = handleCommon.orElse {
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
      else goto(new CompletedWaitingForReady(publisher))

    case CommandComplete(_, rowsAffected) =>
      rowsAffectedPromise.success(rowsAffected.map(_.toLong).getOrElse(0L))
      warningsPromise.success(warnings)
      if (txMgmt) goto(new CompletedPendingCommit(publisher))
      else goto(new CompletedWaitingForReady(publisher))

    case CloseComplete =>
      if (txMgmt) goto(new CompletedPendingCommit(publisher))
      else goto(new CompletedWaitingForReady(publisher))

    case err: StatusMessage.Error if err.isFatal =>
      val ex = PgStmtExecutionException(err.statusData)
      goto(ConnectionClosed(ConnectionClosedException("TODO cause"))) andThen {
        publisher.failure(ex)
        warningsPromise.failure(ex)
        rowsAffectedPromise.failure(ex)
      }
    //TODO after transitioning to connectionclosed, a channel needs to be closed

    //TODO massive code dupl

    case err: StatusMessage.Error =>
      val ex = PgStmtExecutionException(err.statusData)
      goto(Failed(txMgmt) {
        publisher.failure(ex)
        warningsPromise.failure(ex)
        rowsAffectedPromise.failure(ex)
      })
  }

  val name = "extended_querying.pulling_rows"
}
