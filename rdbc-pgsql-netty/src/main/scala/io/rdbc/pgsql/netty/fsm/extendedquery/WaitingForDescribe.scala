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

import io.rdbc.api.exceptions.{ConnectionClosedException, RdbcException}
import io.rdbc.pgsql.core.SessionParams
import io.rdbc.pgsql.core.exception.PgStmtExecutionException
import io.rdbc.pgsql.core.messages.backend._
import io.rdbc.pgsql.core.types.PgTypeRegistry
import io.rdbc.pgsql.netty.exception.ProtocolViolationException
import io.rdbc.pgsql.netty.fsm.ConnectionClosed
import io.rdbc.pgsql.netty.fsm.State.Outcome
import io.rdbc.pgsql.netty.{ChannelWriter, PgResultStream, PgRowPublisher, TimeoutScheduler}
import io.rdbc.sapi.TypeConverterRegistry

import scala.concurrent.{ExecutionContext, Promise}

object WaitingForDescribe {
  def withTxMgmt(portalName: Option[String],
                 streamPromise: Promise[PgResultStream],
                 parsePromise: Promise[Unit],
                 sessionParams: SessionParams,
                 timeoutScheduler: TimeoutScheduler,
                 rdbcTypeConvRegistry: TypeConverterRegistry,
                 pgTypeConvRegistry: PgTypeRegistry)
                (implicit out: ChannelWriter,
                 ec: ExecutionContext): WaitingForDescribe = {
    new WaitingForDescribe(txMgmt = true, portalName, streamPromise, parsePromise, pgTypeConvRegistry, rdbcTypeConvRegistry, sessionParams, timeoutScheduler)
  }

  def withoutTxMgmt(portalName: Option[String],
                    streamPromise: Promise[PgResultStream],
                    parsePromise: Promise[Unit],
                    sessionParams: SessionParams,
                    timeoutScheduler: TimeoutScheduler,
                    rdbcTypeConvRegistry: TypeConverterRegistry,
                    pgTypeConvRegistry: PgTypeRegistry)
                   (implicit out: ChannelWriter,
                    ec: ExecutionContext): WaitingForDescribe = {
    new WaitingForDescribe(txMgmt = false, portalName, streamPromise, parsePromise, pgTypeConvRegistry, rdbcTypeConvRegistry, sessionParams, timeoutScheduler)
  }
}

class WaitingForDescribe protected(txMgmt: Boolean,
                                   portalName: Option[String],
                                   streamPromise: Promise[PgResultStream],
                                   parsePromise: Promise[Unit],
                                   pgTypeConvRegistry: PgTypeRegistry,
                                   rdbcTypeConvRegistry: TypeConverterRegistry,
                                   sessionParams: SessionParams,
                                   timeoutScheduler: TimeoutScheduler
                                  )(implicit out: ChannelWriter, ec: ExecutionContext) extends ExtendedQueryingCommon {

  private var maybeAfterDescData = Option.empty[AfterDescData]

  def handleMsg = handleCommon.orElse {
    case ParseComplete =>
      parsePromise.success(())
      stay

    case BindComplete => stay
    case _: ParameterDescription => stay

    case rowDesc: RowDescription => maybeAfterDescData match {
      case Some(_) => onError(new ProtocolViolationException("already received row description"))
      case None =>
        val publisher = new PgRowPublisher(out, rowDesc, portalName, pgTypeConvRegistry, rdbcTypeConvRegistry, sessionParams, timeoutScheduler)
        val warningsPromise = Promise[Vector[NoticeMessage]]
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

    case _: ReadyForQuery => maybeAfterDescData match {
      case None => throw new ProtocolViolationException("ready for query received without prior row desc")
      case Some(afterDescData@AfterDescData(publisher, _, _)) =>
        goto(new PullingRows(txMgmt, afterDescData)) andThen {
          publisher.resume()
        }
    }

    case err: ErrorMessage if err.isFatal =>
      val ex = PgStmtExecutionException(err.statusData)
      maybeAfterDescData match {
        case Some(AfterDescData(publisher, warningsPromise, rowsAffectedPromise)) =>
          goto(ConnectionClosed(ConnectionClosedException("TODO cause"))) andThen {
            publisher.failure(ex)
            warningsPromise.failure(ex)
            rowsAffectedPromise.failure(ex)
            parsePromise.failure(ex)
          }

        case None =>
          goto(ConnectionClosed(ConnectionClosedException("TODO cause"))) andThen {
            streamPromise.failure(ex)
          }
      }

    case err: ErrorMessage => onError(PgStmtExecutionException(err.statusData))

    //TODO massive code duplication
  }

  private def onError(ex: RdbcException): Outcome = maybeAfterDescData match {
    case Some(AfterDescData(publisher, warningsPromise, rowsAffectedPromise)) =>
      goto(Failed(txMgmt) {
        publisher.failure(ex)
        warningsPromise.failure(ex)
        rowsAffectedPromise.failure(ex)
        parsePromise.failure(ex)
      })

    case None =>
      goto(Failed(txMgmt) {
        streamPromise.failure(ex)
      })
  }

  val shortDesc = "extended_querying.waiting_for_describe"
}
