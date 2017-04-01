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

package io.rdbc.pgsql.core.internal

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import io.rdbc.pgsql.core.exception.PgSubscriptionRejectedException
import io.rdbc.pgsql.core.internal.scheduler.{ScheduledTask, TimeoutHandler}
import io.rdbc.pgsql.core.pgstruct.messages.backend.{DataRow, RowDescription}
import io.rdbc.pgsql.core.pgstruct.messages.frontend._
import io.rdbc.pgsql.core.types.PgTypeRegistry
import io.rdbc.pgsql.core.util.concurrent.LockFactory
import io.rdbc.pgsql.core.{ChannelWriter, FatalErrorNotifier, RequestId, SessionParams}
import io.rdbc.sapi.{Row, TypeConverterRegistry}
import io.rdbc.util.Logging
import org.reactivestreams.{Publisher, Subscriber, Subscription}

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

private[core] object PgRowPublisher {

  object DummySubscription extends Subscription {
    def cancel(): Unit = ()
    def request(n: Long): Unit = ()
  }

  class LockGuardedState(lockFactory: LockFactory) {
    private[this] val lock = lockFactory.lock

    private[this] var ready = false
    private[this] var demand = 0L
    private[this] var unboundedDemand = false

    def ifCanQuery(body: (Option[Int]) => Unit): Unit = {
      val action = lock.withLock {
        if (ready && (demand > 0L || unboundedDemand)) {
          ready = false
          if (unboundedDemand) {
            () => body(None)
          } else {
            val localDemand = demand
            () => body(Some(localDemand.toInt)) //TODO isn't Some(localDemand.toInt) strict? Do I need localDemand?
          }
        } else () => ()
      }
      action()
    }

    def ifCanCancel(body: => Unit): Unit = {
      val can = lock.withLock {
        if (ready) {
          ready = false
          true
        } else false
      }
      if (can) {
        body
      }
    }

    def increaseDemand(n: Long): Unit = {
      lock.withLock {
        try {
          demand = Math.addExact(demand, n)
        } catch {
          case _: ArithmeticException => unboundedDemand = true
        }
      }
    }

    def decrementDemand(): Unit = {
      lock.withLock {
        if (!unboundedDemand) {
          demand = demand - 1L
        }
      }
    }

    def setUnboundedDemand(): Unit = {
      lock.withLock {
        unboundedDemand = true
      }
    }

    def setReady(): Unit = {
      lock.withLock {
        ready = true
      }
    }
  }
}


//TODO in the future tests, use reactive streams TCK to test this publisher & subscription
private[core] class PgRowPublisher(rowDesc: RowDescription,
                                   val portalName: Option[PortalName],
                                   pgTypes: PgTypeRegistry,
                                   typeConverters: TypeConverterRegistry,
                                   sessionParams: SessionParams,
                                   maybeTimeoutHandler: Option[TimeoutHandler],
                                   lockFactory: LockFactory,
                                   @volatile private[this] var fatalErrorNotifier: FatalErrorNotifier)
                                  (implicit reqId: RequestId, out: ChannelWriter, ec: ExecutionContext)
  extends Publisher[Row] with Logging {

  import PgRowPublisher._

  private[this] val subscriber = new AtomicReference(Option.empty[Subscriber[_ >: Row]])
  @volatile private[this] var cancelRequested = false
  private[this] val neverExecuted = new AtomicBoolean(true)
  @volatile private[this] var timeoutScheduledTask = Option.empty[ScheduledTask]
  private[this] val lockGuarded = new LockGuardedState(lockFactory)

  import lockGuarded._

  private[this] val nameIdxMapping: Map[ColName, Int] = {
    Map(rowDesc.colDescs.zipWithIndex.map {
      case (cdesc, idx) => cdesc.name -> idx
    }: _*)
  }

  object RowSubscription extends Subscription {
    def cancel(): Unit = {
      ifCanCancel {
        closePortal()
      }
      cancelRequested = true
    }

    def request(n: Long): Unit = {
      if (n == Long.MaxValue) {
        setUnboundedDemand()
      } else {
        increaseDemand(n)
      }
      tryQuerying()
    }
  }

  def subscribe(s: Subscriber[_ >: Row]): Unit = {
    if (s == null) {
      throw new NullPointerException("Subscriber cannot be null") //spec 1.9
    } else {
      if (subscriber.compareAndSet(None, Some(s))) {
        s.onSubscribe(RowSubscription)
      } else {
        s.onSubscribe(DummySubscription) //spec 1.9
        s.onError(
          new PgSubscriptionRejectedException(
            "This publisher can be subscribed only once, " +
              "it has already been subscribed by other subscriber.")
        )
      }
    }
  }

  def handleRow(dataRow: DataRow): Unit = {
    /* this method is always called by the same I/O thread */
    //TODO should I make it thread-safe anyway?
    cancelTimeoutTask()

    subscriber.get().foreach { s =>
      val pgRow = new PgRow(
        rowDesc = rowDesc,
        cols = dataRow.colValues,
        nameMapping = nameIdxMapping,
        typeConverters = typeConverters,
        pgTypes = pgTypes,
        sessionParams = sessionParams
      )
      decrementDemand()
      s.onNext(pgRow)
    }
  }

  def resume(): Unit = {
    setReady()
    if (cancelRequested) {
      ifCanCancel {
        closePortal()
      }
    } else {
      tryQuerying()
    }
  }

  private def closePortal(): Unit = {
    out.writeAndFlush(ClosePortal(portalName), Sync).recover {
      case NonFatal(ex) =>
        fatalErrNotifier("Write error when closing portal", ex)
    }
  }

  def complete(): Unit = {
    cancelTimeoutTask()
    subscriber.get().foreach(_.onComplete())
  }

  def failure(ex: Throwable): Unit = {
    cancelTimeoutTask()
    subscriber.get().foreach(_.onError(ex))
  }

  private def tryQuerying(): Unit = {
    ifCanQuery { demand =>
      out.writeAndFlush(Execute(portalName, demand), Sync).onComplete {
        case Success(_) =>
          if (neverExecuted.compareAndSet(true, false)) {
            logger.trace(s"Statement was never executed, scheduling a timeout task with handler $maybeTimeoutHandler")
            timeoutScheduledTask = maybeTimeoutHandler.map(_.scheduleTimeoutTask(reqId))
          } else {
            logger.trace("Statement was executed before, not scheduling a timeout task")
          }

        case Failure(NonFatal(ex)) =>
          fatalErrNotifier("Write error occurred when querying", ex)
      }
    }
  }

  private def cancelTimeoutTask(): Unit = {
    timeoutScheduledTask.foreach(_.cancel())
  }

  private[core] def fatalErrNotifier_=(fen: FatalErrorNotifier): Unit = fatalErrorNotifier = fen

  private[core] def fatalErrNotifier: FatalErrorNotifier = fatalErrorNotifier
}
