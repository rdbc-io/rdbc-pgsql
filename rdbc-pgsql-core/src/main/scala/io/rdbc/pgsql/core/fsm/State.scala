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

package io.rdbc.pgsql.core.fsm

import io.rdbc.pgsql.core.fsm.State.{Fatal, Goto, Outcome, Stay}
import io.rdbc.pgsql.core.messages.backend.PgBackendMessage

import scala.concurrent.Promise

object State {
  sealed trait Outcome

  case class Goto(next: State, afterTransition: Option[() => Unit]) extends Outcome {
    def andThen(block: => Unit): Goto = {
      Goto(next, Some(() => block))
    }
  }

  case object Stay extends Outcome

  case class Fatal(ex: Throwable, afterTransition: Option[() => Unit]) extends Outcome {
    def andThen(block: => Unit): Fatal = {
      Fatal(ex, Some(() => block))
    }

    def andThenFailPromise[A](promise: Promise[A]): Fatal = {
      andThen(promise.failure(ex))
    }
  }

  case object Unhandled extends Outcome
}

trait State {
  def handleMsg: PartialFunction[PgBackendMessage, Outcome]
  def name: String
  def stay = Stay
  def fatal(ex: Throwable) = Fatal(ex, None)
  def goto(next: State) = Goto(next, None)
}
