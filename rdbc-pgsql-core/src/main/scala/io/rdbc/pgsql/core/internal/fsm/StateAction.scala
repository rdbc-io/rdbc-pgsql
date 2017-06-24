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

import scala.concurrent.{Future, Promise}

private[core] sealed trait StateAction

private[core] object StateAction {
  case class Goto(next: State, afterTransition: Option[() => Future[Unit]]) extends StateAction {
    def andThen(body: => Future[Unit]): Goto = {
      Goto(next, Some(() => body))
    }
    def andThenF(body: => Unit): Goto = {
      andThen(Future.successful(body))
    }
  }

  case class Stay(afterAcknowledgment: Option[() => Future[Unit]]) extends StateAction {
    def andThen(body: => Future[Unit]): Stay = {
      Stay(Some(() => body))
    }
    def andThenF(body: => Unit): Stay = {
      andThen(Future.successful(body))
    }
  }

  case class Fatal(ex: Throwable, afterRelease: Option[() => Future[Unit]]) extends StateAction {
    def andThen(body: => Future[Unit]): Fatal = {
      Fatal(ex, Some(() => body))
    }

    def andThenF(body: => Unit): Fatal = {
      andThen(Future.successful(body))
    }

    def andThenFailPromise[A](promise: Promise[A]): Fatal = {
      andThen(Future.successful(promise.failure(ex)))
    }
  }
}
