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

import io.rdbc.sapi.{Row, RowPublisher}
import io.rdbc.util.Preconditions.notNull
import org.reactivestreams.{Subscriber, Subscription}

import scala.concurrent.Future

class FailedPgRowPublisher(failure: Throwable)
  extends RowPublisher {

  object DummySubscription extends Subscription {
    def cancel(): Unit = ()

    def request(n: Long): Unit = ()
  }

  val rowsAffected: Future[Nothing] = Future.failed(failure)

  val warnings: Future[Nothing] = Future.failed(failure)

  val metadata: Future[Nothing] = Future.failed(failure)

  def subscribe(s: Subscriber[_ >: Row]): Unit = {
    notNull(s)
    s.onSubscribe(DummySubscription)
    s.onError(failure)
  }
}
