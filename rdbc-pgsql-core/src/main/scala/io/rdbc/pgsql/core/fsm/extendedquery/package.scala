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

import io.rdbc.pgsql.core.PgRowPublisher
import io.rdbc.pgsql.core.fsm.State.Outcome
import io.rdbc.pgsql.core.messages.backend.{PgBackendMessage, StatusMessage}

import scala.concurrent.Promise
import scala.util.control.NonFatal

package object extendedquery {

  trait WarningCollection extends State {
    private var _warnings = Vector.empty[StatusMessage.Notice]

    protected def warnings = _warnings

    abstract override def onMessage(msg: PgBackendMessage): Outcome = {
      try {
        msg match {
          case notice: StatusMessage.Notice if notice.isWarning =>
            _warnings = _warnings :+ notice
            stay
          case _ => super.onMessage(msg)
        }
      } catch {
        case NonFatal(ex) => fatal(ex) andThen onFatalError(ex)
      }
    }
  }

  case class AfterDescData(publisher: PgRowPublisher,
                           warningsPromise: Promise[Vector[StatusMessage.Notice]],
                           rowsAffectedPromise: Promise[Long])
}
