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

package io.rdbc.pgsql.core.internal.fsm

import io.rdbc.pgsql.core._
import io.rdbc.pgsql.core.auth.Authenticator
import io.rdbc.pgsql.core.exception.{PgDriverInternalErrorException, PgStatusDataException}
import io.rdbc.pgsql.core.internal.fsm.streaming._
import io.rdbc.pgsql.core.internal.protocol.TxStatus
import io.rdbc.pgsql.core.internal.protocol.messages.backend._
import io.rdbc.pgsql.core.internal.protocol.messages.frontend.{Bind, Parse, PortalName}
import io.rdbc.pgsql.core.internal.{PgMsgHandler, PgRowPublisher, PortalDescData}
import io.rdbc.sapi.exceptions.ConnectionClosedException
import io.rdbc.util.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

private[core] trait State extends Logging {

  def onMessage(msg: PgBackendMessage): StateAction = traced {
    try {
      msg match {
        case nonFatalErr: StatusMessage.Error if !nonFatalErr.isFatal =>
          onNonFatalError(PgStatusDataException(nonFatalErr.statusData))

        case fatalErr: StatusMessage.Error =>
          val ex = PgStatusDataException(fatalErr.statusData)
          fatal(ex) andThen onFatalErrorF(ex)

        case msg => msgHandler.orElse(fallbackHandler).apply(msg)
      }
    } catch {
      case NonFatal(ex) => fatal(ex) andThen onFatalErrorF(ex)
    }
  }

  private[this] val fallbackHandler: PgMsgHandler = {
    case noticeMsg: StatusMessage.Notice =>
      if (noticeMsg.isWarning) {
        logger.warn(s"Warning received: ${noticeMsg.statusData.shortInfo}")
      } else {
        logger.debug(s"Notice received: ${noticeMsg.statusData.shortInfo}")
      }
      stay

    case NotificationResponse(pid, channel, payload) =>
      logger.info(
        s"Asynchronous notification in channel '$channel' " +
          s"received from pid ${pid.value} " + payload.fold("with no payload")(s => s"with payload '$s'")
      )
      stay

    case unknownMsg: UnknownBackendMessage =>
      val ex = new PgDriverInternalErrorException(
        s"Unknown backend message received: '$unknownMsg'"
      )
      fatal(ex) andThen onFatalErrorF(ex)

    case unhandledMsg =>
      val ex = new PgDriverInternalErrorException(
        s"Unhandled backend message '$unhandledMsg' in state '$this'"
      )
      fatal(ex) andThen onFatalErrorF(ex)
  }

  protected def msgHandler: PgMsgHandler
  protected def onFatalError(ex: Throwable): Unit
  protected def onNonFatalError(ex: Throwable): StateAction

  protected def onFatalErrorF(ex: Throwable): Future[Unit] = Future.successful(onFatalError(ex))
  protected def stay = StateAction.Stay(afterAcknowledgment = None)
  protected def fatal(ex: Throwable) = StateAction.Fatal(ex, afterRelease = None)
  protected def goto(next: State) = StateAction.Goto(next, afterTransition = None)
}

private[core] object State extends Logging {

  def idle(txStatus: TxStatus): Idle = Idle(txStatus)

  def authenticating(initPromise: Promise[BackendKeyData],
                     authenticator: Authenticator)
                    (implicit out: ChannelWriter): Authenticating = {
    new Authenticating(initPromise, authenticator)
  }

  def connectionClosed(cause: ConnectionClosedException): ConnectionClosed = {
    ConnectionClosed(cause)
  }

  def deallocatingStatement(promise: Promise[Unit]): DeallocatingStatement = {
    new DeallocatingStatement(promise)
  }

  def executingBatch(promise: Promise[TxStatus]): ExecutingBatch = {
    new ExecutingBatch(promise)
  }

  val waitingForNextBatch: WaitingForNextBatch = new WaitingForNextBatch

  def executingWriteOnly(parsePromise: Promise[Unit],
                         resultPromise: Promise[Long]): ExecutingWriteOnly = {
    new ExecutingWriteOnly(parsePromise, resultPromise)
  }

  def initializing(initPromise: Promise[BackendKeyData]): Initializing = {
    new Initializing(initPromise)
  }

  def simpleQuerying(promise: Promise[Unit]): SimpleQuerying = {
    new SimpleQuerying(promise)
  }

  val startingRequest = StartingRequest

  val uninitialized = Uninitialized

  def waitingAfterSuccess(promise: Promise[Unit]): WaitingForReady = {
    waitingAfterSuccess(promise, ())
  }

  def waitingAfterSuccess[A](promise: Promise[A], value: A): WaitingForReady = {
    new WaitingForReady(
      onIdle = promise.success(value),
      onFailure = exWhenWaiting => promise.failure(exWhenWaiting)
    )
  }

  def waitingAfterFailure[A](promise: Promise[A], failure: Throwable): WaitingForReady = {
    waitingAfterFailure(ex => promise.failure(ex), failure)
  }

  def waitingAfterFailure(after: Throwable => Unit, failure: Throwable): WaitingForReady = {
    new WaitingForReady(
      onIdle = after(failure),
      onFailure = { exWhenWaiting =>
        logger.error("Error occurred when waiting for ready", exWhenWaiting)
        after(failure)
      }
    )
  }

  object Streaming {

    val waitingForSubscriber = new StrmWaitingForSubscriber

    def beginningTx(maybeParse: Option[Parse],
                    bind: Bind,
                    publisher: PgRowPublisher,
                    describePromise: Promise[PortalDescData],
                    parsePromise: Promise[Unit])
                   (implicit out: ChannelWriter,
                    ec: ExecutionContext): StrmBeginningTx = {
      new StrmBeginningTx(
        maybeParse = maybeParse,
        bind = bind,
        publisher = publisher,
        describePromise = describePromise,
        parsePromise = parsePromise
      )
    }

    def pendingClosePortal(publisher: PgRowPublisher, onIdle: => Unit)
                          (implicit out: ChannelWriter, ec: ExecutionContext): StrmPendingClosePortal = {
      new StrmPendingClosePortal(publisher, onIdle)
    }

    def pendingCommit(publisher: PgRowPublisher)
                     (implicit out: ChannelWriter, ec: ExecutionContext): StrmPendingCommit = {
      new StrmPendingCommit(publisher)
    }

    def pullingRows(txMgmt: Boolean, afterDescData: PortalDescData, publisher: PgRowPublisher)
                   (implicit out: ChannelWriter, ec: ExecutionContext): StrmPullingRows = {
      new StrmPullingRows(txMgmt, afterDescData, publisher)
    }

    def queryFailed(txMgmt: Boolean, portalName: Option[PortalName])(sendFailureCause: => Unit)
                   (implicit out: ChannelWriter, ec: ExecutionContext): StrmQueryFailed = {
      new StrmQueryFailed(txMgmt, portalName, sendFailureCause)
    }

    def waitingAfterClose(onIdle: => Unit,
                          publisher: PgRowPublisher)
                         (implicit out: ChannelWriter, ec: ExecutionContext): StrmWaitingAfterClose = {
      new StrmWaitingAfterClose(onIdle, publisher)
    }

    def waitingAfterCommit(publisher: PgRowPublisher)
                          (implicit out: ChannelWriter, ec: ExecutionContext): StrmWaitingAfterCommit = {
      new StrmWaitingAfterCommit(publisher)
    }

    def waitingAfterRollback(sendFailureCause: => Unit): StrmWaitingAfterRollback = {
      new StrmWaitingAfterRollback(sendFailureCause)
    }

    def waitingForDescribe(txMgmt: Boolean,
                           publisher: PgRowPublisher,
                           portalName: Option[PortalName],
                           describePromise: Promise[PortalDescData],
                           parsePromise: Promise[Unit])
                          (implicit out: ChannelWriter,
                           ec: ExecutionContext): StrmWaitingForDescribe = {
      new StrmWaitingForDescribe(
        txMgmt = txMgmt,
        publisher = publisher,
        portalName = portalName,
        describePromise = describePromise,
        parsePromise = parsePromise
      )
    }

  }

}
