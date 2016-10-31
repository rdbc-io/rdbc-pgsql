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

package io.rdbc.pgsql.core

import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.StrictLogging
import io.rdbc.api.exceptions.{ConnectionClosedException, IllegalSessionStateException}
import io.rdbc.implbase.ConnectionPartialImpl
import io.rdbc.pgsql.core.auth.Authenticator
import io.rdbc.pgsql.core.exception.PgUncategorizedException
import io.rdbc.pgsql.core.fsm._
import io.rdbc.pgsql.core.messages.backend._
import io.rdbc.pgsql.core.messages.frontend.{Query, StartupMessage, Terminate}
import io.rdbc.pgsql.core.scheduler.TaskScheduler
import io.rdbc.pgsql.core.types.PgTypeRegistry
import io.rdbc.sapi.{Connection, Statement, TypeConverterRegistry}
import org.reactivestreams.Publisher

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.stm.{Ref, _}
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * Created by krzysztofpado on 31/10/2016.
  */
//TODO make a note in Connection scaladoc that implementations must be thread safe
abstract class PgConnection(val pgTypeConvRegistry: PgTypeRegistry,
                            val rdbcTypeConvRegistry: TypeConverterRegistry,
                            private[pgsql] val out: ChannelWriter,
                            implicit val ec: ExecutionContext,
                            private[pgsql] val scheduler: TaskScheduler,
                            requestCanceler: (BackendKeyData) => Future[Unit])
  extends Connection
    with ConnectionPartialImpl
    with PgConnectionPartialImpl
    with StrictLogging {

  private val idle: Ref[Boolean] = Ref(false)
  private val handlingTimeout: Ref[Boolean] = Ref(false)
  protected val state: Ref[State] = Ref(Uninitialized: State)
  private val idlePromise: Ref[Promise[this.type]] = Ref(Promise[this.type])

  @volatile private[pgsql] var sessionParams = SessionParams.default
  @volatile private[pgsql] var stmtCache = PreparedStmtCache.empty
  @volatile private var bkd = Option.empty[BackendKeyData]
  private val stmtCounter = new AtomicInteger(0)
  private val lastRequestId: Ref[Long] = Ref(Long.MinValue)

  def watchForIdle: Future[this.type] = idlePromise.single().future

  def statement(sql: String): Future[Statement] = {
    Future.successful(new PgStatement(this, PgNativeStatement.parse(sql)))
  }

  def streamIntoTable(sql: String, paramsPublisher: Publisher[Map[String, Any]]): Future[Unit] = ???

  protected def simpleQueryIgnoreResult(sql: String)(implicit timeout: FiniteDuration): Future[Unit] = ifReady { (reqId, _) =>
    //TODO timeout
    val queryPromise = Promise[Unit]
    triggerTransition(new SimpleQuerying.PullingRows(out, queryPromise))
    out.writeAndFlush(Query(sql))
    queryPromise.future.flatMap(_ => watchForIdle.map(_ => ()))
  }

  private[pgsql] def init(dbUser: String, dbName: String, authenticator: Authenticator): Future[Unit] = {
    logger.debug(s"Initializing connection")
    val initPromise = Promise[BackendKeyData]
    triggerTransition(new Authenticating(initPromise, authenticator)(out, ec))
    out.writeAndFlush(StartupMessage(dbUser, dbName))
    initPromise.future.map { returnedBkd =>
      bkd = Some(returnedBkd)
      ()
    }
  }

  private[pgsql] def triggerTransition(newState: State, afterTransition: Option[() => Unit] = None): Unit = {
    val successful = atomic { implicit txn =>
      state() match {
        case ConnectionClosed(_) => false
        case _ =>
          logger.trace(s"Transitioning from state '${state()}' to '$newState'")
          newState match {
            case Idle(_) =>
              idlePromise().success(this)
              idle() = true

            case ConnectionClosed(cause) =>
              if (!idlePromise().isCompleted) {
                idlePromise().failure(cause)
              }
              idle() = false

            case _ => ()
          }
          state() = newState
          true
      }
    }
    if (successful) {
      afterTransition.foreach(_.apply())
    }
  }

  private def handleCharsetChange(pgCharsetName: String)(consumer: (Charset) => Unit) = {
    PgCharset.toJavaCharset(pgCharsetName) match {
      case Some(charset) => consumer(charset)
      case None =>
        val msg = s"Unsupported charset '$pgCharsetName'"
        logger.error(msg)
        doRelease(msg)
    }
  }

  def clientCharsetChanged(charset: Charset): Unit

  def serverCharsetChanged(charset: Charset): Unit

  protected def onUnhandled(msg: PgBackendMessage, localState: State): Unit = msg match {
    case ParameterStatus("client_encoding", pgCharsetName) => handleCharsetChange(pgCharsetName) { charset =>
      clientCharsetChanged(charset)
      sessionParams = sessionParams.copy(clientCharset = charset)
    }

    case ParameterStatus("server_encoding", pgCharsetName) => handleCharsetChange(pgCharsetName) { charset =>
      serverCharsetChanged(charset)
      sessionParams = sessionParams.copy(serverCharset = charset)
    }

    case ParameterStatus(name, value) => logger.debug(s"Received parameter '$name' = '$value'")

    case err: ErrorMessage =>
      logger.error(s"Unhandled error message received: ${err.statusData.shortInfo}")
      val ex = new PgUncategorizedException(err.statusData)
      doRelease(ex)

    case statusMsg: StatusMessage =>
      if (statusMsg.isWarning) {
        logger.warn(s"Warning received: ${statusMsg.statusData.shortInfo}")
      } else {
        logger.debug(s"Notice received: ${statusMsg.statusData.shortInfo}")
      }

    case unknownMsg: UnknownPgMessage =>
      val msg = s"Unknown message received: '$unknownMsg'"
      logger.error(msg)
      doRelease(msg)

    case unhandledMsg =>
      val msg = s"Unhandled message '$unhandledMsg' in state '$localState'"
      logger.error(msg)
      doRelease(msg)
  }

  private[pgsql] def onTimeout[A](reqId: Long): Unit = {
    val shouldCancel = atomic { implicit txn =>
      if (!handlingTimeout() && !idle() && lastRequestId() == reqId) {
        handlingTimeout() = true
        true
      } else false
    }
    if (shouldCancel) {
      logger.debug(s"Timeout occurred for request '$reqId', cancelling it")
      bkd.foreach { bkd =>
        requestCanceler(bkd).onComplete(_ => handlingTimeout.single() = false)
      }
    }
  }

  private[pgsql] def nextStmtName(): String = "S" + stmtCounter.incrementAndGet()

  private[pgsql] def ifReady[A](block: (Long, TxStatus) => Future[A]): Future[A] = {
    val action = atomic { implicit txn =>
      if (idle() && !handlingTimeout()) {
        state() match {
          case Idle(txStatus) =>
            idle() = false
            state() = StartingRequest
            idlePromise() = Promise[this.type]
            lastRequestId() = lastRequestId() + 1L
            val localLastRequestId = lastRequestId()
            () => block(localLastRequestId, txStatus)

          case _ => ??? //TODO fatal error bug
        }
      } else state() match {
        case ConnectionClosed(cause) => () => Future.failed(cause)
        case _ => () => Future.failed(IllegalSessionStateException(s"Session is busy"))
      }
    }
    action()
  }

  def release(): Future[Unit] = ifReady { (_, _) =>
    logger.debug(s"Releasing connection on client request")
    doRelease("Connection released by client")
  }

  protected def doRelease(cause: Throwable): Future[Unit] = {
    out.writeAndFlush(Terminate).map { _ =>
      out.close()
      val connClosedEx = cause match {
        case ex: ConnectionClosedException => ex
        case ex => ConnectionClosedException("Connection closed", ex)
      }
      triggerTransition(ConnectionClosed(connClosedEx))
    }
  }

  protected def doRelease(cause: String): Future[Unit] = {
    doRelease(ConnectionClosedException(cause))
  }

  def forceRelease(): Future[Unit] = {
    logger.info("Forcing a connection release")
    doRelease("Connection released by client (forced)")
  }
}
