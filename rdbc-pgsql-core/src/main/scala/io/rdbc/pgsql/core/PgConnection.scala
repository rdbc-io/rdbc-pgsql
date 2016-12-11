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

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.scalalogging.StrictLogging
import io.rdbc._
import io.rdbc.api.exceptions.{ConnectionClosedException, IllegalSessionStateException}
import io.rdbc.implbase.{ConnectionPartialImpl, ReturningInsertImpl}
import io.rdbc.pgsql.core.auth.Authenticator
import io.rdbc.pgsql.core.fsm.State.{Fatal, Goto, Stay}
import io.rdbc.pgsql.core.fsm._
import io.rdbc.pgsql.core.fsm.extendedquery.batch.ExecutingBatch
import io.rdbc.pgsql.core.fsm.extendedquery.writeonly.ExecutingWriteOnly
import io.rdbc.pgsql.core.fsm.extendedquery.{BeginningTx, WaitingForDescribe}
import io.rdbc.pgsql.core.messages.backend._
import io.rdbc.pgsql.core.messages.frontend._
import io.rdbc.pgsql.core.scheduler.{TaskScheduler, TimeoutScheduler}
import io.rdbc.pgsql.core.types.PgTypeRegistry
import io.rdbc.pgsql.core.util.SleepLock
import io.rdbc.sapi._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

trait PgStatementExecutor {
  def executeStatementForStream(nativeSql: String, params: ImmutSeq[DbValue])(implicit timeout: FiniteDuration): Future[ResultStream]
  def executeStatementForRowsAffected(nativeSql: String, params: ImmutSeq[DbValue])(implicit timeout: FiniteDuration): Future[Long]
  def executeParamsStream(nativeSql: String, params: Source[ImmutSeq[DbValue], NotUsed]): Future[Unit]
}

trait PgStatementDeallocator {
  def deallocateStatement(nativeSql: String): Future[Unit]
}


//TODO make a note in Connection scaladoc that implementations must be thread safe
abstract class PgConnection(val pgTypeConvRegistry: PgTypeRegistry,
                            val rdbcTypeConvRegistry: TypeConverterRegistry,
                            private[pgsql] val out: ChannelWriter,
                            implicit val ec: ExecutionContext,
                            private[pgsql] val scheduler: TaskScheduler,
                            requestCanceler: (BackendKeyData) => Future[Unit],
                            implicit val streamMaterializer: Materializer)
  extends Connection
    with ConnectionPartialImpl
    with StrictLogging {

  thisConn =>

  class FsmManager {
    private[this] val lock = new SleepLock //TODO make lock impl configurable

    private[this] var ready = false
    private[this] var handlingTimeout = false
    private[this] var state: State = Uninitialized
    private[this] var readyPromise = Promise[thisConn.type]
    private[this] var lastRequestId = Long.MinValue

    def ifReady[A](block: (Long, TxStatus) => Future[A]): Future[A] = {
      val action: () => Future[A] = lock.withLock {
        if (handlingTimeout) {
          () => Future.failed(new IllegalSessionStateException(s"Session is busy, currently cancelling timed out action"))
        } else if (ready) {
          state match {
            case Idle(txStatus) =>
              ready = false
              state = StartingRequest
              readyPromise = Promise[thisConn.type]
              lastRequestId = lastRequestId + 1L
              val localLastRequestId = lastRequestId
              () => block(localLastRequestId, txStatus)

            case _ => ??? //TODO fatal error bug
          }
        } else {
          state match {
            case ConnectionClosed(cause) => () => Future.failed(cause)
            case _ => () => Future.failed(new IllegalSessionStateException(s"Session is busy, currently processing query"))
          }
        }
      }
      action()
    }

    def triggerTransition(newState: State, afterTransition: Option[() => Future[Unit]] = None): Unit = {
      val successful = lock.withLock {
        state match {
          case ConnectionClosed(_) => false
          case _ =>
            newState match {
              case Idle(_) =>
                ready = true
                readyPromise.success(thisConn)

              case ConnectionClosed(cause) =>
                ready = false
                if (!readyPromise.isCompleted) {
                  readyPromise.failure(cause)
                }

              case _ => ()
            }
            state = newState
            true
        }
      }
      if (successful) {
        logger.trace(s"Transitioned to state '$newState'")
        //TODO note that afterTransition action can't access state or any transactional data
        afterTransition.foreach(_.apply().recover {
          case NonFatal(ex) => onFatalError("Fatal error occurred in handling after state transition logic", ex)
        })
      }
    }

    def onTimeout[A](reqId: Long): Unit = {
      val shouldCancel = lock.withLock {
        if (!handlingTimeout && !ready && lastRequestId == reqId) {
          handlingTimeout = true
          true
        } else false
      }
      if (shouldCancel) {
        logger.debug(s"Timeout occurred for request '$reqId', cancelling it")
        bkd.foreach { bkd =>
          requestCanceler(bkd).onComplete(_ => lock.withLock(handlingTimeout = false))
        }
      }
    }

    def currentState: State = lock.withLock(state)

    def readyFuture: Future[thisConn.type] = lock.withLock(readyPromise.future)

  }

  private val fsmManager = new FsmManager

  @volatile private var sessionParams = SessionParams.default
  @volatile private var stmtCache = PreparedStmtCache.empty
  @volatile private var bkd = Option.empty[BackendKeyData]
  private val stmtCounter = new AtomicInteger(0)

  def watchForIdle: Future[this.type] = fsmManager.readyFuture

  def statement(sql: String): Future[AnyStatement] = {
    Future.successful(new PgAnyStatement(stmtExecutor, stmtDeallocator, pgTypeConvRegistry, sessionParams, PgNativeStatement.parse(sql)))
  }

  def beginTx()(implicit timeout: FiniteDuration): Future[Unit] = simpleQueryIgnoreResult("BEGIN")
  def commitTx()(implicit timeout: FiniteDuration): Future[Unit] = simpleQueryIgnoreResult("COMMIT")
  def rollbackTx()(implicit timeout: FiniteDuration): Future[Unit] = simpleQueryIgnoreResult("ROLLBACK")
  def returningInsert(sql: String): Future[ReturningInsert] = returningInsert(sql, "*")

  def validate()(implicit timeout: FiniteDuration): Future[Boolean] = simpleQueryIgnoreResult("").map(_ => true).recoverWith {
    case ex: IllegalSessionStateException => Future.failed(ex)
    case _ => Future.successful(false)
  }

  def returningInsert(sql: String, keyColumns: String*): Future[ReturningInsert] = {
    val returningSql = sql + " returning " + keyColumns.mkString(",")
    statement(returningSql).map { stmt =>
      new ReturningInsertImpl(stmt)
    }
  }

  protected def simpleQueryIgnoreResult(sql: String)(implicit timeout: FiniteDuration): Future[Unit] = fsmManager.ifReady { (reqId, _) =>
    //TODO timeout
    val queryPromise = Promise[Unit]
    fsmManager.triggerTransition(new SimpleQuerying.PullingRows(out, queryPromise))
    out.writeAndFlush(Query(sql)).recoverWith(writeFailureHandler)
      .flatMap(_ => queryPromise.future)
    //TODO common handler for ChannelErrors -> close conn & treat as fatal
    //TODO handle TCP disconnects
  }

  private[pgsql] def init(dbUser: String, dbName: String, authenticator: Authenticator): Future[Unit] = {
    logger.debug(s"Initializing connection")
    val initPromise = Promise[BackendKeyData]
    fsmManager.triggerTransition(new Authenticating(initPromise, authenticator)(out, ec))

    out.writeAndFlush(StartupMessage(dbUser, dbName)).recoverWith(writeFailureHandler).flatMap { _ =>
      initPromise.future.map { returnedBkd =>
        bkd = Some(returnedBkd)
        ()
      }
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

  protected def clientCharsetChanged(charset: Charset): Unit

  protected def serverCharsetChanged(charset: Charset): Unit

  protected def onMessage(msg: PgBackendMessage): Unit = {
    logger.trace(s"Received backend message '$msg'")
    msg match {
      case ParameterStatus("client_encoding", pgCharsetName) => handleCharsetChange(pgCharsetName) { charset =>
        clientCharsetChanged(charset)
        sessionParams = sessionParams.copy(clientCharset = charset)
      }

      case ParameterStatus("server_encoding", pgCharsetName) => handleCharsetChange(pgCharsetName) { charset =>
        serverCharsetChanged(charset)
        sessionParams = sessionParams.copy(serverCharset = charset)
      }

      case ParameterStatus(name, value) => logger.debug(s"Received parameter '$name' = '$value'")

      case _ =>
        fsmManager.currentState.onMessage(msg) match {
          case Stay => ()
          case Goto(newState, afterTransitionAction) => fsmManager.triggerTransition(newState, afterTransitionAction)
          case Fatal(ex, afterReleaseAction) =>
            logger.error("Fatal error occurred, connection will be closed", ex)
            doRelease(ex).map(_ => afterReleaseAction.foreach(_.apply()))
        }
    }
  }

  private[pgsql] def nextStmtName(): String = "S" + stmtCounter.incrementAndGet()

  def release(): Future[Unit] = fsmManager.ifReady { (_, _) =>
    logger.debug(s"Releasing connection on client request")
    doRelease("Connection released by client")
  }

  protected def doRelease(cause: Throwable): Future[Unit] = {
    out.writeAndFlush(Terminate).recover {
      case writeEx =>
        logger.error("Write error occurred when terminating connection", writeEx)
        ()
    }.map { _ =>
      out.close()
      //TODO this block needs to happen regardless of the success of Terminate write, so can't use map here
      val connClosedEx = cause match {
        case ex: ConnectionClosedException => ex
        case ex => new ConnectionClosedException("Connection closed", ex)
      }
      fsmManager.triggerTransition(ConnectionClosed(connClosedEx))
    }
  }

  private def onWriteError(cause: Throwable): Unit = {
    onFatalError("Write error occurred, the connection will be closed", cause)
  }

  private def onFatalError(msg: String, cause: Throwable): Unit = {
    logger.error(msg, cause)
    out.close()
    fsmManager.triggerTransition(ConnectionClosed(new ConnectionClosedException("Connection closed", cause)))
  }

  protected def doRelease(cause: String): Future[Unit] = {
    doRelease(new ConnectionClosedException(cause))
  }


  def forceRelease(): Future[Unit] = {
    logger.info("Forcing a connection release")
    doRelease("Connection released by client (forced)")
  }

  private val stmtDeallocator = new PgStatementDeallocator {
    def deallocateStatement(nativeSql: String): Future[Unit] = fsmManager.ifReady { (_, txStatus) =>
      stmtCache.get(nativeSql) match {
        case Some(stmtName) =>
          val promise = Promise[Unit]
          fsmManager.triggerTransition(new DeallocatingStatement(promise))
          out.writeAndFlush(CloseStatement(Some(stmtName)), Sync).recoverWith(writeFailureHandler)
            .flatMap(_ => promise.future)

        case None =>
          fsmManager.triggerTransition(Idle(txStatus))
          Future.successful(())
      }
    }
  }

  private val stmtExecutor = new PgStatementExecutor {
    def executeStatementForStream(nativeSql: String, params: ImmutSeq[DbValue])
                                 (implicit timeout: FiniteDuration): Future[ResultStream] = {
      //TODO close portal after completion?
      fsmManager.ifReady { (reqId, txStatus) =>
        logger.debug(s"Executing statement '$nativeSql'")
        val (parse, bind) = parseAndBind(nativeSql, params)

        val streamPromise = Promise[PgResultStream]
        val parsePromise = Promise[Unit]

        val timeoutScheduler = TimeoutScheduler {
          scheduler.schedule(timeout) {
            fsmManager.onTimeout(reqId)
          }
        }

        val writeFut = txStatus match {
          case TxStatus.Active =>
            fsmManager.triggerTransition(WaitingForDescribe.withoutTxMgmt(bind.portal, streamPromise, parsePromise, sessionParams,
              timeoutScheduler, rdbcTypeConvRegistry, pgTypeConvRegistry, onFatalError)(out, ec))
            parse.foreach(out.write(_))
            out.writeAndFlush(bind, Describe(PortalType, bind.portal), Sync)

          case TxStatus.Idle =>
            fsmManager.triggerTransition(BeginningTx(parse, bind, streamPromise, parsePromise, sessionParams, timeoutScheduler, rdbcTypeConvRegistry, pgTypeConvRegistry, onFatalError)(out, ec))
            out.writeAndFlush(Query("BEGIN"))

          case TxStatus.Failed => Future.successful(()) //TODO oooo
        }

        writeFut.recoverWith(writeFailureHandler).flatMap { _ =>
          parse.flatMap(_.optionalName).foreach { stmtName =>
            parsePromise.future.foreach { _ =>
              stmtCache = stmtCache.updated(nativeSql, stmtName)
            }
          }
          streamPromise.future
        }
      }
    }

    protected def parseAndBind(nativeSql: String, params: ImmutSeq[DbValue]): (Option[Parse], Bind) = {
      val cachedPreparedStatement = stmtCache.get(nativeSql)
      //TODO can't use the same cached prepared statement for other param types. nativeSql + paramTypes have to be the cache key
      val (stmtName, parse) = if (cachedPreparedStatement.isDefined) {
        (cachedPreparedStatement, Option.empty[Parse])
      } else {
        val stmtName = if (shouldCache()) Some(nextStmtName()) else None
        val parse = Some(Parse(stmtName, nativeSql, params.map(_.dataTypeOid).toVector))
        (stmtName, parse)
      }

      //TODO AllTextual TODO toList
      (parse, Bind(stmtName.map(_ + "P"), stmtName, params.toList, AllBinary))
    }

    protected def shouldCache(): Boolean = {
      //TODO introduce a cache threshold
      true
    }

    def executeStatementForRowsAffected(nativeSql: String, params: ImmutSeq[DbValue])(implicit timeout: FiniteDuration): Future[Long] = {
      fsmManager.ifReady { (reqId, _) =>
        logger.debug(s"Executing write-only statement '$nativeSql'")
        val (parse, bind) = parseAndBind(nativeSql, params)

        val timeoutScheduler = TimeoutScheduler {
          scheduler.schedule(timeout) {
            fsmManager.onTimeout(reqId)
          }
        }
        timeoutScheduler.scheduleTimeout()
        //TODO code dupl

        val promise = Promise[Long]
        fsmManager.triggerTransition(new ExecutingWriteOnly(promise))

        parse.foreach(out.write(_))
        out.writeAndFlush(bind.copy(portal = None), Execute(None, None), Sync).recoverWith(writeFailureHandler).flatMap { _ =>
          val fut = promise.future

          fut.map { rowsAffected =>
            parse.flatMap(_.optionalName).foreach { stmtName =>
              stmtCache = stmtCache.updated(nativeSql, stmtName)
            }
            rowsAffected
          }
        }
      }
    }

    def executeParamsStream(nativeSql: String, paramsSource: Source[ImmutSeq[DbValue], NotUsed]): Future[Unit] = {
      fsmManager.ifReady { (_, _) =>
        val promise = Promise[Unit]

        val stmtName = Option.empty[String]
        val portalName = Option.empty[String]
        val execute = Execute(portalName, None)
        var first = true
        //TODO make max batch size configurable
        paramsSource.batch(100L, first => Vector(first))((acc, elem) => acc :+ elem).mapAsyncUnordered(1) { batch =>
          val batchPromise = Promise[TxStatus]
          fsmManager.triggerTransition(new ExecutingBatch(batchPromise))

          if (first) {
            val parse = Parse(stmtName, nativeSql, batch.head.map(_.dataTypeOid).toVector) //TODO use cached value if available
            out.write(parse)
            first = false
          }

          batch.foreach { oneParamSet =>
            out.write(Bind(portalName, stmtName, oneParamSet.toList, AllBinary), execute)
          }

          out.writeAndFlush(Sync).recoverWith(writeFailureHandler)
            .flatMap(_ => batchPromise.future)

        }.runWith(Sink.last).onComplete {
          case Success(txStatus) => fsmManager.triggerTransition(
            newState = Idle(txStatus),
            afterTransition = Some(() => Future.successful(promise.success(())))
          )

          case Failure(ex) => promise.failure(ex)
        }

        promise.future
      }
    }
  }

  private def writeFailureHandler[T]: PartialFunction[Throwable, Future[T]] = {
    case writeEx =>
      logger.error("Write error occurred, connection will be closed", writeEx)
      onWriteError(writeEx)
      Future.failed(writeEx)
  }

}
