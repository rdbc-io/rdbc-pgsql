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

package io.rdbc.pgsql.netty

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import io.rdbc.api.exceptions.RdbcException
import io.rdbc.pgsql.core.messages.backend.{DataRow, RowDescription}
import io.rdbc.pgsql.core.messages.frontend.{ClosePortal, Execute, Sync}
import io.rdbc.pgsql.core.types.PgTypeRegistry
import io.rdbc.pgsql.core.{PgRow, SessionParams}
import io.rdbc.pgsql.netty.scheduler.ScheduledTask
import io.rdbc.sapi.{Row, TypeConverterRegistry}
import org.reactivestreams.{Publisher, Subscriber, Subscription}

import scala.concurrent.stm._

object TimeoutScheduler {
  def apply(f: => ScheduledTask): TimeoutScheduler = {
    new TimeoutScheduler() {
      def scheduleTimeout(): ScheduledTask = f
    }
  }
}

trait TimeoutScheduler extends (() => ScheduledTask) {
  def scheduleTimeout(): ScheduledTask
  def apply(): ScheduledTask = scheduleTimeout()
}

class PgRowPublisher(out: ChannelWriter,
                     rowDesc: RowDescription,
                     portalName: Option[String],
                     pgTypeConvRegistry: PgTypeRegistry,
                     rdbcTypeConvRegistry: TypeConverterRegistry,
                     sessionParams: SessionParams,
                     timeoutScheduler: TimeoutScheduler
                    ) extends Publisher[Row] {

  private val subscriber = new AtomicReference(Option.empty[Subscriber[_ >: Row]])
  @volatile private var cancelRequested = false
  private val neverExecuted = new AtomicBoolean(true)
  @volatile private var timeoutScheduledTask = Option.empty[ScheduledTask]
  private val state = new State()

  private val nameIdxMapping: Map[String, Int] = {
    Map(rowDesc.fieldDescriptions.zipWithIndex.map {
      case (fdesc, idx) => fdesc.name -> idx
    }: _*)
  }

  object RowSubscription extends Subscription {
    def cancel(): Unit = {
      state.ifCanCancel {
        doCancel()
      }
      cancelRequested = true
    }

    def request(n: Long): Unit = {
      if (n == Long.MaxValue) {
        state.setUnboundedDemand()
      } else {
        state.increaseDemand(n)
      }
      tryQuerying()
    }
  }

  def subscribe(s: Subscriber[_ >: Row]): Unit = {
    if (subscriber.compareAndSet(None, Some(s))) {
      s.onSubscribe(RowSubscription)
    } else {
      s.onError(new RuntimeException("max 1 subscriber/can subscribe only once")) //TODO
    }
  }

  private[netty] def handleRow(dataRow: DataRow): Unit = {
    //this method is always called by the same I/O thread
    cancelTimeoutScheduler()

    subscriber.get().foreach { s =>
      val pgRow = new PgRow(
        rowDesc = rowDesc,
        cols = dataRow.fieldValues,
        nameMapping = nameIdxMapping,
        rdbcTypeConvRegistry = rdbcTypeConvRegistry,
        pgTypeConvRegistry = pgTypeConvRegistry,
        sessionParams = sessionParams
      )
      state.decrementDemand()
      s.onNext(pgRow)
    }
  }

  private[netty] def resume(): Unit = {
    state.setReady()
    if (cancelRequested) {
      println("resume try cancelling")
      state.ifCanCancel {
        doCancel()
      }
    } else {
      println("resume tryQuerying")
      tryQuerying()
    }
  }

  private def doCancel(): Unit = {
    out.writeAndFlush(ClosePortal(portalName), Sync)
  }

  private[netty] def complete(): Unit = {
    cancelTimeoutScheduler()
    subscriber.get().foreach(_.onComplete())
  }

  private[netty] def failure(ex: RdbcException): Unit = {
    cancelTimeoutScheduler()
    subscriber.get().foreach(_.onError(ex))
  }

  private def tryQuerying(): Unit = {
    state.ifCanQuery { demand =>
      out.writeAndFlush(Execute(portalName, demand), Sync)
      if (neverExecuted.compareAndSet(true, false)) {
        timeoutScheduledTask = Some(timeoutScheduler.scheduleTimeout())
      }
    }
  }

  private def cancelTimeoutScheduler(): Unit = {
    timeoutScheduledTask.foreach(_.cancel())
  }

  class State() {
    private val ready: Ref[Boolean] = Ref(false)
    private val demand: Ref[Long] = Ref(0L)
    private val unboundedDemand: Ref[Boolean] = Ref(false)

    def ifCanQuery(block: (Option[Int]) => Unit): Unit = {
      val action = atomic { implicit txn =>
        if (ready() && (demand() > 0L || unboundedDemand())) {
          ready() = false
          if (unboundedDemand()) {
            () => block(None)
          } else {
            val localDemand = demand()
            () => block(Some(localDemand.toInt))
          }
        } else () => ()
      }
      action()
    }

    def ifCanCancel(block: => Unit): Unit = {
      val can = atomic { implicit txn =>
        if (ready()) {
          ready() = false
          true
        } else false
      }
      if (can) {
        block
      }
    }

    def increaseDemand(n: Long): Unit = {
      atomic { implicit txn =>
        demand() = demand() + n //TODO handle overflows
      }
    }

    def decrementDemand(): Unit = {
      atomic { implicit txn =>
        if (!unboundedDemand()) {
          demand() = demand() - 1L
        }
      }
    }

    def setUnboundedDemand(): Unit = {
      unboundedDemand.single() = true
    }

    def setReady(): Unit = {
      ready.single() = true
    }
  }

}
