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

import io.rdbc.pgsql.core.messages.backend.{ActiveTxStatus, ReadyForQuery}
import io.rdbc.pgsql.core.messages.frontend.Query
import io.rdbc.pgsql.netty.{ChannelWriter, PgNettyConnection, PgRowPublisher}

import scala.concurrent.{ExecutionContext, Future}

class CompletedPendingCommit(publisher: PgRowPublisher)(implicit out: ChannelWriter, ec: ExecutionContext) extends ExtendedQueryingCommon {

  def handleMsg = {
    case ReadyForQuery(ActiveTxStatus) =>
      goto(new CompletedWaitingForReadyAfterCommit(publisher)) andThen {
        out.writeAndFlush(Query("COMMIT"))
      }
  }

  val shortDesc = "extended_querying.pending_commit"
}
