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

package io.rdbc.pgsql.core.internal

import io.rdbc.api.exceptions.IllegalSessionStateException
import io.rdbc.pgsql.core.exception.PgDriverInternalErrorException
import io.rdbc.pgsql.core.internal.fsm._
import io.rdbc.pgsql.core.{ClientRequest, ConnId, RequestId}
import io.rdbc.util.Logging

import scala.concurrent.stm._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

private[core] class PgSessionFsmManager(connId: ConnId,
                                        fatalErrorHandler: FatalErrorHandler
                                       )(implicit val ec: ExecutionContext)
  extends Logging {
  private[this] val ready = Ref(false)
  private[this] val handlingTimeout = Ref(false)
  private[this] val state: Ref[State] = Ref(State.uninitialized: State)
  private[this] val readyPromise = Ref(Promise[Unit])
  private[this] val lastRequestId = Ref(RequestId(connId, 0L))

  /* TODO can't make this traced, compilation fails, investigate */
  def ifReady[A](request: ClientRequest[A]): A = {
    val action: () => A = atomic { implicit tx =>
      if (handlingTimeout()) {
        () =>
          throw new IllegalSessionStateException(s"Session is busy, currently cancelling timed out action")
      } else if (ready()) {
        actionWhenReady(request)
      } else {
        actionWhenNotReady()
      }
    }
    action()
  }

  def ifReadyF[A](request: ClientRequest[Future[A]]): Future[A] = {
    Try(ifReady(request)) match {
      case Success(res) => res
      case Failure(ex) => Future.failed(ex)
    }
  }

  /* TODO can't make this traced, compilation fails, investigate */
  private def actionWhenReady[A](request: ClientRequest[A])(implicit txn: InTxn): () => A = {
    state() match {
      case Idle(txStatus) =>
        val newRequestId = prepareStateForNewRequest()
        () => request(newRequestId, txStatus)

      case state =>
        val ex = new PgDriverInternalErrorException(s"Expected connection state to be idle, actual state was $state")
        fatalErrorHandler.handleFatalError(ex.getMessage, ex)
        () => throw ex
    }
  }

  private def actionWhenNotReady[A]()(implicit txn: InTxn): () => A = traced {
    state() match {
      case ConnectionClosed(cause) =>
        () => throw cause
      case _ =>
        () => throw new IllegalSessionStateException(s"Session is busy, currently processing query")
    }
  }

  private def prepareStateForNewRequest()(implicit txn: InTxn): RequestId = traced {
    ready() = false
    state() = StartingRequest
    readyPromise() = Promise[Unit]
    lastRequestId() = lastRequestId().copy(value = lastRequestId().value + 1L)
    lastRequestId()
  }

  def triggerTransition(newState: State, afterTransition: Option[() => Future[Unit]] = None): Boolean = traced {
    val transitioned = atomic { implicit tx =>
      state() match {
        case ConnectionClosed(_) => false
        case _ =>
          newState match {
            case Idle(_) => ready() = true
            case ConnectionClosed(_) => ready() = false
            case _ => ()
          }
          state() = newState
          true
      }
    }
    if (transitioned) {
      logger.debug(s"Transitioned to state '$newState'")
      newState match {
        case Idle(_) => readyPromise.single().success(())
        case _ => ()
      }
      runAfterTransition(afterTransition)
    }
    transitioned
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
    atomic { implicit txn =>
      if (!handlingTimeout() && !ready() && lastRequestId() == reqId) {
        handlingTimeout() = true
        true
      } else false
    }
  }

  def finishHandlingTimeout(): Unit = traced {
    handlingTimeout.single() = false
  }

  def currentState: State = traced(state.single())

  def readyFuture: Future[Unit] = traced(readyPromise.single().future)

}
