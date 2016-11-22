package io.rdbc.pgsql.core.fsm.extendedquery.batch

import io.rdbc.pgsql.core.fsm.State.Outcome
import io.rdbc.pgsql.core.fsm.{State, WaitingForReady}
import io.rdbc.pgsql.core.messages.backend._

import scala.concurrent.Promise

class ExecutingBatch(promise: Promise[TxStatus]) extends State {

  protected def msgHandler = {
    case ParseComplete => stay
    case BindComplete => stay
    case _: DataRow => stay
    case EmptyQueryResponse | _: CommandComplete => stay
    case ReadyForQuery(txStatus) =>
      promise.success(txStatus)
      stay
  }

  protected def onFatalError(ex: Throwable): Unit = promise.failure(ex)

  protected def onNonFatalError(ex: Throwable): Outcome = {
    goto(new WaitingForReady(onIdle = promise.failure(ex), onFailure = { exWhenWaiting =>
      logger.error("Error occurred when waiting for ready", exWhenWaiting)
      promise.failure(ex)
    })) //TODO this repeats throughout the project
  }

  val name = "executing_batch"
}
