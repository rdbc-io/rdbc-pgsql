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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import io.rdbc.ImmutSeq
import io.rdbc.pgsql.core._
import io.rdbc.pgsql.core.exception.PgSubscriptionRejectedException
import io.rdbc.pgsql.core.internal.PgRowPublisher.PublisherState.{Cancelling, IdleReadyToPull}
import io.rdbc.pgsql.core.pgstruct.messages.backend.{DataRow, RowDescription}
import io.rdbc.pgsql.core.pgstruct.messages.frontend._
import io.rdbc.pgsql.core.types.PgTypeRegistry
import io.rdbc.sapi.{ColumnMetadata, Row, RowMetadata, RowPublisher, TypeConverterRegistry, Warning}
import io.rdbc.util.Preconditions.checkNotNull
import io.rdbc.util.Logging
import io.rdbc.util.scheduler.ScheduledTask
import org.reactivestreams.{Subscriber, Subscription}

import scala.concurrent.stm._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

private[core] object PgRowPublisher {

  object DummySubscription extends Subscription {
    def cancel(): Unit = ()
    def request(n: Long): Unit = ()
  }

  trait PublisherState
  object PublisherState {
    case object Uninitialized extends PublisherState
    case object PreparingPortal extends PublisherState
    case object IdleReadyToPull extends PublisherState
    case object PulledRows extends PublisherState
    case object Cancelling extends PublisherState
    case object Complete extends PublisherState
    case object Errored extends PublisherState
  }

  class ConcurrentState
    extends Logging {

    private[this] val publisherState: Ref[PublisherState] = {
      Ref(PublisherState.Uninitialized: PublisherState)
    }

    private[this] val demand = Ref(0L)
    private[this] val unboundedDemand = Ref(false)

    def ifUninitialized(body: () => Unit): Unit = {
      val action = atomic { implicit tx =>
        publisherState() match {
          case PublisherState.Uninitialized =>
            setState(PublisherState.PreparingPortal)
            () => body()
          case _ => () => ()
        }
      }
      action()
    }

    def ifCanPullRows(body: (Option[Int]) => Unit): Unit = {
      val action = atomic { implicit tx =>
        publisherState() match {
          case IdleReadyToPull if demand() > 0L || unboundedDemand() =>
            setState(PublisherState.PulledRows)
            if (unboundedDemand()) {
              () => body(None)
            } else {
              val localDemand = demand()
              () => body(Some(localDemand.toInt)) //TODO isn't Some(localDemand.toInt) strict? Do I need localDemand?
            }
          case _ => () => ()
        }
      }
      action()
    }

    def ifCanCancel(body: PublisherState => Unit): Unit = {
      val (can, state) = atomic { implicit tx =>
        publisherState() match {
          case state@(PublisherState.IdleReadyToPull | PublisherState.Uninitialized) =>
            setState(Cancelling)
            (true, state)
          case _ => (false, publisherState())
        }
      }
      if (can) {
        body(state)
      }
    }

    def increaseDemand(n: Long): Unit = {
      atomic { implicit tx =>
        try {
          demand() = Math.addExact(demand(), n)
        } catch {
          case _: ArithmeticException => unboundedDemand() = true
        }
      }
    }

    def decrementDemand(): Unit = {
      atomic { implicit tx =>
        if (!unboundedDemand()) {
          demand() = demand() - 1L
        }
      }
    }

    def setUnboundedDemand(): Unit = {
      atomic { implicit tx =>
        unboundedDemand() = true
      }
    }

    def setIdleReadyToPull(): Unit = {
      atomic { implicit tx =>
        setState(PublisherState.IdleReadyToPull)
      }
    }

    def setComplete(): Unit = {
      atomic { implicit tx =>
        setState(PublisherState.Complete)
      }
    }

    def setErrored(): Unit = {
      atomic { implicit tx =>
        setState(PublisherState.Errored)
      }
    }

    private def setState(newState: PublisherState)(implicit txn: InTxn): Unit = {
      logger.debug(s"Publisher transitioning from ${publisherState()} to $newState")
      publisherState() = newState
    }
  }

}

private[core] class PgRowPublisher(preparePortal: (PgRowPublisher) => Future[PortalDescData],
                                   val portalName: Option[PortalName],
                                   pgTypes: PgTypeRegistry,
                                   typeConverters: TypeConverterRegistry,
                                   sessionParams: SessionParams,
                                   maybeTimeoutHandler: Option[TimeoutHandler],
                                   @volatile private[this] var fatalErrorNotifier: FatalErrorNotifier)
                                  (implicit reqId: RequestId, out: ChannelWriter, ec: ExecutionContext)
  extends RowPublisher with Logging {

  import PgRowPublisher._

  private[this] val portalDescDataPromise = Promise[PortalDescData]
  private[this] val subscriber = new AtomicReference(Option.empty[Subscriber[_ >: Row]])
  @volatile private[this] var cancelRequested = false
  private[this] val neverExecuted = new AtomicBoolean(true)
  @volatile private[this] var timeoutScheduledTask = Option.empty[ScheduledTask]
  private[this] val concurrentState = new ConcurrentState()
  private[this] val donePromise = Promise[Unit]

  import concurrentState._

  object RowSubscription extends Subscription {
    def cancel(): Unit = {
      ifCanCancel {
        case PublisherState.Uninitialized => ()
        case _ =>
          closePortal()
          ()
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
    checkNotNull(s)
    if (subscriber.compareAndSet(None, Some(s))) {
      s.onSubscribe(RowSubscription)
    } else {
      s.onSubscribe(DummySubscription) //spec 1.9
      s.onError(
        new PgSubscriptionRejectedException(
          "This publisher can be subscribed to only once, " +
            "it has already been subscribed by other subscriber.")
      )
    }
  }

  def handleRow(dataRow: DataRow,
                rowDesc: RowDescription,
                nameIdxMapping: Map[ColName, Int]): Unit = {
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
    setIdleReadyToPull()
    if (cancelRequested) {
      ifCanCancel { //TODO code dupl
        case PublisherState.Uninitialized => ()
        case _ =>
          closePortal()
          ()
      }
    } else {
      tryQuerying()
    }
  }

  private def closePortal(): Future[Unit] = {
    out.writeAndFlush(ClosePortal(portalName), Sync).recover {
      case NonFatal(ex) =>
        fatalErrNotifier("Write error when closing portal", ex)
    }
  }

  def complete(): Unit = {
    cancelTimeoutTask()
    setComplete()
    subscriber.get().foreach(_.onComplete())
    donePromise.success(())
  }

  def failure(ex: Throwable): Unit = {
    cancelTimeoutTask()
    setErrored()
    subscriber.get().foreach(_.onError(ex))
    donePromise.failure(ex)
  }

  private def tryQuerying(): Unit = {
    ifUninitialized { () =>
      val preparePortalFut = preparePortal(this)
      portalDescDataPromise.completeWith(preparePortalFut)
      ()
    }
    ifCanPullRows { demand =>
      out.writeAndFlush(Execute(portalName, demand), Sync).onComplete {
        case Success(_) =>
          //TODO should timeout task beb started on preparePortal or before execute?
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

  override val rowsAffected: Future[Long] = {
    portalDescDataPromise.future.flatMap { descData =>
      descData.rowsAffectedPromise.future
    }
  }

  override val warnings: Future[ImmutSeq[Warning]] = {
    portalDescDataPromise.future.flatMap { descData =>
      descData.warningsPromise.future.map { warnMsgs =>
        warnMsgs.map { warnMsg =>
          Warning(warnMsg.statusData.shortInfo, warnMsg.statusData.sqlState)
        }
      }
    }
  }

  override val metadata: Future[RowMetadata] = {
    portalDescDataPromise.future.map { descData =>
      val rowDesc = descData.rowDesc
      val columnsMetadata = rowDesc.colDescs.map { colDesc =>
        ColumnMetadata(
          name = colDesc.name.value,
          dbTypeId = colDesc.dataType.oid.value.toString,
          cls = pgTypes.typeByOid(colDesc.dataType.oid).map(_.cls)
        )
      }
      RowMetadata(columnsMetadata)
    }
  }

  override val done: Future[Unit] = donePromise.future
}
