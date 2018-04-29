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

import io.rdbc.pgsql.core.exception.PgSubscriptionRejectedException
import io.rdbc.sapi.{Row, RowPublisher}
import io.rdbc.util.Preconditions.checkNotNull
import org.reactivestreams.{Subscriber, Subscription}

import scala.concurrent.Future

class FailedPgRowPublisher(failure: Throwable)
  extends RowPublisher {

  private[this] val subscribed = new AtomicBoolean(false)
  private[this] val failedFuture = Future.failed(failure)

  object DummySubscription extends Subscription {
    def cancel(): Unit = ()

    def request(n: Long): Unit = ()
  }

  val rowsAffected: Future[Nothing] = failedFuture

  val warnings: Future[Nothing] = failedFuture

  val metadata: Future[Nothing] = failedFuture

  val done: Future[Nothing] = failedFuture

  def subscribe(s: Subscriber[_ >: Row]): Unit = {
    checkNotNull(s)
    s.onSubscribe(DummySubscription)
    if (subscribed.compareAndSet(false, true)) {
      s.onError(failure)
    } else {
      s.onError(
        new PgSubscriptionRejectedException(
          "This publisher can be subscribed to only once, " +
            "it has already been subscribed by other subscriber.")
      )
    }
  }
}
