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

import java.util.concurrent.atomic.AtomicBoolean

import io.rdbc.pgsql.core.pgstruct.{Argument, TxStatus}
import io.rdbc.pgsql.core.internal.Compat._
import io.rdbc.util.Futures._
import io.rdbc.util.Logging
import io.rdbc.util.Preconditions._
import org.reactivestreams.{Subscriber, Subscription}

import scala.concurrent.stm._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

private[core]
class StatementArgsSubscriber[T](nativeStmt: PgNativeStatement,
                                 bufferCapacity: Int,
                                 minDemandRequest: Int,
                                 initialTxStatus: TxStatus,
                                 batchExecutor: BatchExecutor,
                                 argConverter: T => Try[Vector[Argument]]
                                )(implicit private val ec: ExecutionContext)
  extends Subscriber[T]
    with Logging {

  check(minDemandRequest, minDemandRequest <= bufferCapacity,
    "min demand request has to be less or equal to buffer capacity"
  ) //TODO move this check to config?

  private val donePromise = Promise[Unit]

  private val buffer = Ref(Vector.empty[Vector[Argument]])
  private val connIdle = Ref(true)
  private val executedBefore = Ref(false)
  private val lastExecution = Ref(Future.unit)

  private val publisherErrored = Ref(false)

  private val executingCount = Ref(0)
  private val remainingRequested = Ref(0)
  private val canceled = Ref(false)

  private val subscribed = new AtomicBoolean(false)

  @volatile private var lastTxStatus = initialTxStatus
  @volatile private var subscription = Option.empty[Subscription]

  val done: Future[Unit] = donePromise.future

  def onNext(elem: T): Unit = traced {
    notNull(elem)
    logger.trace(s"Element received: '$elem'")
    if (!canceled.single()) {
      logger.trace(s"Element '$elem' rejected because subscription has already been canceled")
      argConverter(elem).map { args =>
        atomic { implicit tx =>
          buffer() = buffer() :+ args
          remainingRequested() = remainingRequested() - 1
        }
        executeIfCan()
        requestMore()
      }.recover(handleArgConversionError())
      ()
    }
  }

  private def handleArgConversionError(): PartialFunction[Throwable, Unit] = {
    case ex =>
      val publisherAlreadyErrored = atomic { implicit tx =>
        if (!publisherErrored()) {
          publisherErrored() = true
          false
        } else true
      }

      if (!publisherAlreadyErrored) {
        logWarnException("Cancelling subscription because of arg converter error", ex)
        cancelSubscription()
        lastExecution.single().foreach { _ =>
          batchExecutor.completeBatch(lastTxStatus)
          donePromise.failure(ex)
        }
      } else {
        logWarnException("Swallowing arg conversion error because publisher already errored", ex)
      }
  }

  def onSubscribe(s: Subscription): Unit = traced {
    notNull(s)
    if (subscribed.compareAndSet(false, true)) {
      logger.debug(s"Subscribtion $s successfully subscribed")
      subscription = Some(s)
      requestMore()
    } else {
      logger.debug(s"Rejecting subscribtion $s")
      s.cancel()
    }
  }

  private def requestMore(): Unit = {
    val demand = atomic { implicit tx =>
      val demand = requestDemand()
      remainingRequested() = remainingRequested() + demand
      demand
    }
    if (demand > 0) {
      logger.debug(s"Requesting $demand elements")
      subscription.foreach(_.request(demand.toLong))
    }
  }

  case class Execution(batch: Vector[Vector[Argument]],
                       firstBatch: Boolean,
                       promise: Promise[Unit])

  private def prepareExecution(): Option[Execution] = traced {
    atomic { implicit tx =>
      if (!publisherErrored() && connIdle() && buffer().nonEmpty) {
        val batch = buffer()
        executingCount() = batch.size
        buffer() = Vector.empty
        connIdle() = false
        val firstBatch = if (!executedBefore()) {
          executedBefore() = true
          true
        } else false

        val execution = Execution(batch, firstBatch, Promise[Unit])
        lastExecution() = execution.promise.future

        Some(execution)
      } else None
    }
  }

  private def executeIfCan(): Future[Unit] = traced {
    val maybeExecution = prepareExecution()

    maybeExecution.map { case Execution(batch, firstBatch, promise) =>
      logger.debug(s"Executing batch of size ${batch.size}")
      batchExecutor.executeBatch(nativeStmt, batch, first = firstBatch).andThen {
        case Success(txStatus) =>
          lastTxStatus = txStatus
          atomic { implicit tx =>
            connIdle() = true
            executingCount() = 0
          }
          promise.success(())
          requestMore()
          executeIfCan()

        case Failure(ex) =>
          logWarnException("Batch execution failed", ex)
          promise.failure(ex)
          if (!publisherErrored.single()) {
            cancelSubscription()
            donePromise.failure(ex)
          } else {
            logWarnException("Swallowing batch execution error because publisher failed already", ex)
          }
      }.flatMap(_ => promise.future)
    }.getOrElse(Future.unit)
  }

  def onError(t: Throwable): Unit = traced {
    notNull(t)
    logWarnException("Publisher signalled error", t)
    publisherErrored.single() = true
    lastExecution.single().andThen { case _ =>
      batchExecutor.completeBatch(lastTxStatus)
      donePromise.failure(t)
    }
    ()
  }

  def onComplete(): Unit = traced {
    logger.debug("Publisher completed")
    lastExecution.single().andThenF { case _ =>
      executeIfCan().andThen { case _ =>
        batchExecutor.completeBatch(lastTxStatus)
        donePromise.success(())
      }
    }
    ()
  }

  private def requestDemand()(implicit txn: InTxn): Int = {
    val inFlight = buffer().size + executingCount()
    if ((remainingRequested() + inFlight) <= (bufferCapacity - minDemandRequest)) {
      math.max(0, bufferCapacity - remainingRequested() - inFlight)
    } else 0
  }

  private def cancelSubscription(): Unit = traced {
    subscription.foreach(_.cancel())
    atomic { implicit tx =>
      canceled() = true
    }
  }

  private def logWarnException(msg: String, ex: Throwable): Unit = {
    if (logger.underlying.isDebugEnabled) {
      logger.warn(msg, ex)
    } else {
      logger.warn(s"$msg: ${ex.getMessage}")
    }
  }

  override lazy val toString: String = {
    s"Subscriber($nativeStmt)"
  }
}
