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

package io.rdbc.pgsql.core

import java.util.concurrent.{Executors, ThreadFactory}

import io.rdbc.pgsql.core.internal.{BatchExecutor, PgNativeStatement, StatementArgsSubscriber}
import io.rdbc.pgsql.core.pgstruct.{Argument, Oid, TxStatus}
import org.reactivestreams.tck.SubscriberWhiteboxVerification.{SubscriberPuppet, WhiteboxSubscriberProbe}
import org.reactivestreams.tck.{SubscriberWhiteboxVerification, TestEnvironment}
import org.reactivestreams.{Subscriber, Subscription}
import org.scalatest.testng.TestNGSuiteLike

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

class StmtArgsSubscriberVerification
  extends SubscriberWhiteboxVerification[String](new TestEnvironment())
    with TestNGSuiteLike {

  class DummyBatchExecutor extends BatchExecutor {

    private implicit val ec = ExecutionContext.fromExecutorService(
      Executors.newCachedThreadPool(new ThreadFactory {
        def newThread(r: Runnable): Thread = {
          val thread = new Thread(r)
          thread.setDaemon(true)
          thread
        }
      })
    )

    private def delay(multiplier: Int): Future[Unit] = {
      Future {
        Thread.sleep(multiplier * 10L)
      }
    }

    private[core] def executeBatch(nativeStmt: PgNativeStatement,
                                   batch: Vector[Vector[Argument]],
                                   first: Boolean): Future[TxStatus] = {
      delay(batch.size).map(_ => TxStatus.Idle)
    }

    private[core] def completeBatch(txStatus: TxStatus) = ()
  }

  def createSubscriber(probe: WhiteboxSubscriberProbe[String]): Subscriber[String] = {
    new StatementArgsSubscriber(
      nativeStmt = PgNativeStatement.parse(RdbcSql("insert into tst values(0)")),
      bufferCapacity = 100,
      minDemandRequest = 5,
      initialTxStatus = TxStatus.Idle,
      batchExecutor = new DummyBatchExecutor,
      argConverter = (s: String) => Success(Vector(Argument.Textual(s, Oid.unknownDataType)))
    )(ExecutionContext.global) {

      override def onSubscribe(s: Subscription): Unit = {
        super.onSubscribe(s)

        probe.registerOnSubscribe(new SubscriberPuppet() {
          def signalCancel(): Unit = s.cancel()
          def triggerRequest(elements: Long): Unit = s.request(elements)
        })
      }

      override def onNext(elem: String): Unit = {
        super.onNext(elem)
        probe.registerOnNext(elem)
      }

      override def onError(t: Throwable): Unit = {
        super.onError(t)
        probe.registerOnError(t)
      }

      override def onComplete(): Unit = {
        super.onComplete()
        probe.registerOnComplete()
      }
    }
  }

  def createElement(element: Int): String = element.toString
}
