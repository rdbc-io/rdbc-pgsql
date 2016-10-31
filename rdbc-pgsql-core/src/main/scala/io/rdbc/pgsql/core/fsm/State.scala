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

import io.rdbc.pgsql.core.fsm.State.{Goto, Outcome, Stay}
import io.rdbc.pgsql.core.messages.backend.PgBackendMessage

object State {
  sealed trait Outcome

  case class Goto(next: State, afterTransition: Option[() => Unit]) extends Outcome {
    def andThen(block: => Unit): Goto = {
      Goto(next, Some(() => block))
    }
  }

  case object Stay extends Outcome
  case object Unhandled extends Outcome
}

trait State {
  def handleMsg: PartialFunction[PgBackendMessage, Outcome]
  def shortDesc: String
  def stay = Stay
  def goto(next: State) = Goto(next, None)
}
