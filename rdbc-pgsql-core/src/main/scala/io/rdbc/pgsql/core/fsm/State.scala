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

import com.typesafe.scalalogging.StrictLogging
import io.rdbc.pgsql.core.exception.PgStatusDataException
import io.rdbc.pgsql.core.fsm.State._
import io.rdbc.pgsql.core.messages.backend.{PgBackendMessage, StatusMessage, UnknownBackendMessage}

import scala.concurrent.Promise
import scala.util.control.NonFatal

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
}

trait State extends StrictLogging {

  def onMessage(msg: PgBackendMessage): Outcome = {
    try {
      val outcome = msg match {
        case err: StatusMessage.Error if !err.isFatal =>
          val ex = PgStatusDataException(err.statusData)
          Some(onNonFatalError(ex))

        case err: StatusMessage.Error =>
          val ex = PgStatusDataException(err.statusData)
          Some(fatal(ex) andThen onFatalError(ex))

        case any => msgHandler.lift.apply(any)
      }

      outcome match {
        case None => msg match {
          case noticeMsg: StatusMessage.Notice =>
            if (noticeMsg.isWarning) {
              logger.warn(s"Warning received: ${noticeMsg.statusData.shortInfo}")
            } else {
              logger.debug(s"Notice received: ${noticeMsg.statusData.shortInfo}")
            }
            stay

          case unknownMsg: UnknownBackendMessage =>
            val msg = s"Unknown message received: '$unknownMsg'"
            val ex = new RuntimeException(msg) //TODO internal error
            fatal(ex) andThen onFatalError(ex)

          case unhandledMsg =>
            val msg = s"Unhandled message '$unhandledMsg' in state '$name'"
            val ex = new RuntimeException(msg) //TODO internal error
            fatal(ex) andThen onFatalError(ex)
        }

        case Some(handled) => handled
      }

    } catch {
      case NonFatal(ex) => fatal(ex) andThen onFatalError(ex)
    }
  }

  def name: String

  protected def msgHandler: PartialFunction[PgBackendMessage, Outcome]
  protected def onFatalError(ex: Throwable): Unit
  protected def onNonFatalError(ex: Throwable): Outcome

  protected def stay = Stay
  protected def fatal(ex: Throwable) = Fatal(ex, None)
  protected def goto(next: State) = Goto(next, None)
}

trait DefaultErrorHandling extends NonFatalErrorsAreFatal {
  this: State =>

  protected def onFatalError(ex: Throwable): Unit = ()
}

trait NonFatalErrorsAreFatal {
  this: State =>

  protected def onNonFatalError(ex: Throwable): Outcome = {
    logger.debug(s"State '$name' does not override non-fatal error handler, treating error as fatal")
    fatal(ex) andThen onFatalError(ex)
  }
}