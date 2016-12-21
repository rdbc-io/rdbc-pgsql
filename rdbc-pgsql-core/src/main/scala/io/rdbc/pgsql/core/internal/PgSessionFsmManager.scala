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

package io.rdbc.pgsql.core.internal

import io.rdbc.api.exceptions.IllegalSessionStateException
import io.rdbc.pgsql.core.exception.PgDriverInternalErrorException
import io.rdbc.pgsql.core.internal.fsm._
import io.rdbc.pgsql.core.util.concurrent.LockFactory
import io.rdbc.pgsql.core.{ClientRequest, RequestId}
import io.rdbc.util.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

private[core] class PgSessionFsmManager(lockFactory: LockFactory,
                                        fatalErrorHandler: FatalErrorHandler
                                       )(implicit val ec: ExecutionContext)
  extends Logging {
  private[this] val lock = lockFactory.lock
  private[this] var ready = false
  private[this] var handlingTimeout = false
  private[this] var state: State = Uninitialized
  private[this] var readyPromise = Promise[Unit]
  private[this] var lastRequestId = RequestId(0L)

  /* TODO can't make this traced, compilation fails, investigate */
  def ifReady[A](request: ClientRequest[A]): Future[A] = {
    val action: () => Future[A] = lock.withLock {
      if (handlingTimeout) {
        () =>
          Future.failed {
            new IllegalSessionStateException(s"Session is busy, currently cancelling timed out action")
          }
      } else if (ready) {
        actionWhenReady(request)
      } else {
        actionWhenNotReady()
      }
    }
    action()
  }

  /* TODO can't make this traced, compilation fails, investigate */
  private def actionWhenReady[A](request: ClientRequest[A]): () => Future[A] = {
    state match {
      case Idle(txStatus) =>
        val newRequestId = prepareStateForNewRequest()
        () => request(newRequestId, txStatus)

      case state =>
        val ex = new PgDriverInternalErrorException(s"Expected connection state to be idle, actual state was $state")
        fatalErrorHandler.handleFatalError(ex.getMessage, ex)
        () => Future.failed(ex)
    }
  }

  private def actionWhenNotReady[A](): () => Future[A] = traced {
    state match {
      case ConnectionClosed(cause) =>
        () => Future.failed(cause)
      case _ =>
        () => Future.failed(new IllegalSessionStateException(s"Session is busy, currently processing query"))
    }
  }

  private def prepareStateForNewRequest(): RequestId = traced {
    ready = false
    state = StartingRequest
    readyPromise = Promise[Unit]
    lastRequestId = RequestId(lastRequestId.value + 1L)
    lastRequestId
  }

  def triggerTransition(newState: State, afterTransition: Option[() => Future[Unit]] = None): Boolean = traced {
    val successful = lock.withLock {
      state match {
        case ConnectionClosed(_) => false
        case _ =>
          newState match {
            case Idle(_) =>
              ready = true
              readyPromise.success(())

            case ConnectionClosed(cause) =>
              ready = false
              if (!readyPromise.isCompleted) {
                readyPromise.failure(cause)
              }

            case _ => ()
          }
          state = newState
          true
      }
    }
    if (successful) {
      logger.debug(s"Transitioned to state '$newState'")
      runAfterTransition(afterTransition)
    }
    successful
  }

  private def runAfterTransition(afterTransition: Option[() => Future[Unit]]): Unit = {
    afterTransition.foreach(_.apply()
      .recover {
        case NonFatal(ex) =>
          fatalErrorHandler.handleFatalError(
            "Fatal error occurred when handling post state transition logic", ex
          )
      })
  }

  def startHandlingTimeout(reqId: RequestId): Boolean = traced {
    lock.withLock {
      if (!handlingTimeout && !ready && lastRequestId == reqId) {
        handlingTimeout = true
        true
      } else false
    }
  }

  def finishHandlingTimeout(): Unit = traced {
    lock.withLock(handlingTimeout = false)
  }

  def currentState: State = traced(lock.withLock(state))

  def readyFuture: Future[Unit] = traced(lock.withLock(readyPromise.future))

}
