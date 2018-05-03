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

import io.rdbc.pgsql.core.internal.protocol.messages.backend.{PgBackendMessage, StatusMessage}

import scala.util.control.NonFatal

private[core] trait WarningCollection extends State {
  @volatile private[this] var _warnings = Vector.empty[StatusMessage.Notice]

  protected def warnings: Vector[StatusMessage.Notice] = _warnings

  abstract override def onMessage(msg: PgBackendMessage): StateAction = {
    try {
      msg match {
        case notice: StatusMessage.Notice if notice.isWarning =>
          _warnings = _warnings :+ notice
          stay
        case _ => super.onMessage(msg)
      }
    } catch {
      case NonFatal(ex) => fatal(ex) andThenF onFatalError(ex)
    }
  }
}
