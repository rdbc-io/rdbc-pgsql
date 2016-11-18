package io.rdbc.pgsql.core.fsm.extendedquery.writeonly

import io.rdbc.pgsql.core.fsm.State.Outcome
import io.rdbc.pgsql.core.fsm.{State, WaitingForReady}
import io.rdbc.pgsql.core.messages.backend._

import scala.concurrent.Promise

class ExecutingWriteOnly(promise: Promise[Long]) extends State {
  val name = "executing_write_only"

  protected def msgHandler = {
    case BindComplete => stay
    case ParseComplete => stay
    case _: DataRow => stay
    case EmptyQueryResponse => finished(0L)
    case CommandComplete(_, rowsAffected) => finished(rowsAffected.map(_.toLong).getOrElse(0L))
  }

  private def finished(rowsAffected: Long): Outcome = {
    goto(new WaitingForReady(
      onIdle = promise.success(rowsAffected),
      onFailure = { ex =>
        promise.failure(ex)
      })
    )
  }

  protected def onFatalError(ex: Throwable): Unit = promise.failure(ex)

  protected def onNonFatalError(ex: Throwable): Outcome = {
    goto(new WaitingForReady(onIdle = promise.failure(ex), onFailure = { exWhenWaiting =>
      logger.error("Error occurred when waiting for ready", exWhenWaiting)
      promise.failure(ex)
    })) //TODO this repeats throughout the project
  }
}
