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

package io.rdbc.pgsql.netty

import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.StrictLogging
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.rdbc.api.exceptions.{ConnectionClosedException, IllegalSessionStateException}
import io.rdbc.implbase.ConnectionPartialImpl
import io.rdbc.pgsql.core.auth.Authenticator
import io.rdbc.pgsql.core.exception.PgUncategorizedException
import io.rdbc.pgsql.core.messages.backend._
import io.rdbc.pgsql.core.messages.frontend.{Query, StartupMessage, Terminate}
import io.rdbc.pgsql.core.types.PgTypeRegistry
import io.rdbc.pgsql.core.{PgCharset, PgConnectionBase, PgNativeStatement, SessionParams}
import io.rdbc.pgsql.netty.StatusMsgUtil.isWarning
import io.rdbc.pgsql.netty.codec.{NettyPgMsgDecoder, NettyPgMsgEncoder}
import io.rdbc.pgsql.netty.fsm.State._
import io.rdbc.pgsql.netty.fsm.{Authenticating, _}
import io.rdbc.sapi._
import org.reactivestreams.Publisher

import scala.concurrent.duration._
import scala.concurrent.stm._
import scala.concurrent.{ExecutionContext, Future, Promise}

//TODO make a note in Connection scaladoc that implementations must be thread safe
class PgNettyConnection(val pgTypeConvRegistry: PgTypeRegistry,
                        val rdbcTypeConvRegistry: TypeConverterRegistry,
                        protected[netty] val out: ChannelWriter,
                        decoder: NettyPgMsgDecoder,
                        encoder: NettyPgMsgEncoder,
                        implicit val ec: ExecutionContext,
                        val scheduler: TaskScheduler,
                        private val requestCanceler: (BackendKeyData) => Future[Unit])
  extends SimpleChannelInboundHandler[PgBackendMessage]
    with Connection
    with ConnectionPartialImpl
    with PgConnectionBase
    with StrictLogging {

  private val idle: Ref[Boolean] = Ref(false)
  private val handlingTimeout: Ref[Boolean] = Ref(false)
  private val state: Ref[State] = Ref(Uninitialized: State)
  private val idlePromise: Ref[Promise[this.type]] = Ref(Promise[this.type])

  @volatile private[netty] var sessionParams = SessionParams.default
  @volatile private[netty] var stmtCache = PreparedStmtCache.empty
  private val stmtCounter = new AtomicInteger(0)
  @volatile private var bkd = Option.empty[BackendKeyData]

  private val lastRequestId: Ref[Long] = Ref(Long.MinValue)

  def watchForIdle: Future[this.type] = idlePromise.single().future

  def statement(sql: String): Future[Statement] = {
    Future.successful(new PgNettyStatement(this, PgNativeStatement.fromRdbc(sql)))
  }

  def streamIntoTable(sql: String, paramsPublisher: Publisher[Map[String, Any]]): Future[Unit] = ???

  protected def simpleQueryIgnoreResult(sql: String)(implicit timeout: FiniteDuration): Future[Unit] = ifReady { (reqId, _) =>
    //TODO timeout
    val queryPromise = Promise[Unit]
    triggerTransition(new SimpleQuerying.PullingRows(out, queryPromise))
    out.writeAndFlush(Query(sql))
    queryPromise.future.flatMap(_ => watchForIdle.map(_ => ()))
  }

  private[netty] def init(dbUser: String, dbName: String, authenticator: Authenticator): Future[Unit] = {
    logger.debug(s"Initializing connection")
    val initPromise = Promise[BackendKeyData]
    triggerTransition(new Authenticating(out, initPromise, authenticator))
    out.writeAndFlush(StartupMessage(dbUser, dbName))
    initPromise.future.map { returnedBkd =>
      bkd = Some(returnedBkd)
      ()
    }
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: PgBackendMessage): Unit = {
    logger.trace(s"Received backend message '$msg'")
    val localState = state.single()
    localState.handleMsg.applyOrElse(msg, (_: PgBackendMessage) => Unhandled) match {
      case Unhandled => onUnhandled(msg, localState)
      case Stay => ()
      case Goto(newState, afterTransitionAction) => triggerTransition(newState, afterTransitionAction)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    logger.error("Unhandled exception occurred", cause)
    doRelease(cause)
  }

  private[netty] def triggerTransition(newState: State, afterTransition: Option[() => Unit] = None): Unit = {
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
      case None => doRelease(s"Unsupported charset '$pgCharsetName'")
    }
  }

  private def onUnhandled(msg: PgBackendMessage, localState: State): Unit = msg match {
    case ParameterStatus("client_encoding", pgCharsetName) => handleCharsetChange(pgCharsetName) { charset =>
      encoder.charset = charset
      sessionParams = sessionParams.copy(clientCharset = charset)
    }

    case ParameterStatus("server_encoding", pgCharsetName) => handleCharsetChange(pgCharsetName) { charset =>
      decoder.charset = charset
      sessionParams = sessionParams.copy(serverCharset = charset)
    }

    case ParameterStatus(name, value) => logger.debug(s"Received parameter '$name' = '$value'")

    case err: ErrorMessage =>
      logger.error(s"Unhandled error received: ${err.statusData}")
      val ex = new PgUncategorizedException(err.statusData)
      doRelease(ex)

    case statusMsg: StatusMessage =>
      if (isWarning(statusMsg)) {
        logger.warn(s"Warning received: ${statusMsg.statusData.shortInfo}")
      } else {
        logger.debug(s"Notice received: ${statusMsg.statusData.shortInfo}")
      }

    case unknownMsg: UnknownPgMessage =>
      logger.error(s"Unknown message received: '$unknownMsg'")
      doRelease(s"Unknown message received: '$unknownMsg'")

    case unhandledMsg =>
      logger.error(s"Unhandled message '$unhandledMsg' in state '$localState'")
      doRelease(s"Unhandled message '$unhandledMsg' in state '$localState'")
  }

  private[netty] def onTimeout[A](reqId: Long): Unit = {
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

  private[netty] def nextStmtName(): String = "S" + stmtCounter.incrementAndGet()

  private[netty] def ifReady[A](block: (Long, TxStatus) => Future[A]): Future[A] = {
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

  private def doRelease(cause: Throwable): Future[Unit] = {
    out.writeAndFlush(Terminate).map { _ =>
      out.close()
      val connClosedEx = cause match {
        case ex: ConnectionClosedException => ex
        case ex => ConnectionClosedException("Connection closed", ex)
      }
      triggerTransition(ConnectionClosed(connClosedEx))
    }
  }

  private def doRelease(cause: String): Future[Unit] = {
    doRelease(ConnectionClosedException(cause))
  }

  def forceRelease(): Future[Unit] = {
    doRelease("Connection released by client (forced)")
  }
}
