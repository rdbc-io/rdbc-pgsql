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

package io.rdbc.pgsql.core

import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicInteger

import io.rdbc.api.exceptions.{ConnectionClosedException, ConnectionValidationException, IllegalSessionStateException}
import io.rdbc.implbase.ConnectionPartialImpl
import io.rdbc.pgsql.core.StmtCacheConfig.{Disabled, Enabled}
import io.rdbc.pgsql.core.auth.Authenticator
import io.rdbc.pgsql.core.exception.PgUnsupportedCharsetException
import io.rdbc.pgsql.core.internal._
import io.rdbc.pgsql.core.internal.Compat._
import io.rdbc.pgsql.core.internal.cache.LruStmtCache
import io.rdbc.pgsql.core.internal.fsm.StateAction.{Fatal, Goto, Stay}
import io.rdbc.pgsql.core.internal.fsm._
import io.rdbc.pgsql.core.internal.fsm.streaming.{StrmBeginningTx, StrmWaitingForDescribe}
import io.rdbc.pgsql.core.pgstruct.messages.backend.{SessionParamKey, _}
import io.rdbc.pgsql.core.pgstruct.messages.frontend._
import io.rdbc.pgsql.core.pgstruct.{Argument, ReturnColFormats, TxStatus}
import io.rdbc.sapi._
import io.rdbc.util.Logging
import io.rdbc.util.Preconditions._
import io.rdbc.util.scheduler.TaskScheduler
import org.reactivestreams.Publisher

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try
import scala.util.control.NonFatal

abstract class AbstractPgConnection(val id: ConnId,
                                    config: PgConnectionConfig,
                                    implicit private[this] val out: ChannelWriter,
                                    implicit protected val ec: ExecutionContext,
                                    scheduler: TaskScheduler,
                                    requestCanceler: RequestCanceler)
  extends Connection
    with ConnectionPartialImpl
    with WriteFailureHandler
    with FatalErrorHandler
    with PgStatementExecutor
    with BatchExecutor
    with Logging {

  private[this] val fsmManager = new PgSessionFsmManager(id, this)
  @volatile private[this] var sessionParams = SessionParams.default
  @volatile private[this] var maybeBackendKeyData = Option.empty[BackendKeyData]
  private[this] val stmtCounter = new AtomicInteger(0)

  private[this] def argConverter = new StmtArgConverter(config.pgTypes, sessionParams)

  @volatile private[this] var maybeStmtCache = config.stmtCacheConfig match {
    case Disabled => None
    case Enabled(capacity) => Some(LruStmtCache.empty(capacity))
  }

  override def watchForIdle: Future[this.type] = fsmManager.readyFuture.map(_ => this)

  override def statement(sql: String, options: StatementOptions): Statement = traced {
    argsNotNull()
    checkNonEmptyString(sql)
    val StatementOptions(keyColumns) = options
    val finalSql = keyColumns match {
      case KeyColumns.None => sql
      case KeyColumns.All => s"$sql returning *"
      case KeyColumns.Named(cols) => sql + " returning " + cols.mkString(",")
    }
    new PgStatement(
      stmtExecutor = this,
      nativeStmt = PgNativeStatement.parse(RdbcSql(finalSql)),
      argConverter = argConverter
    )
  }

  override def beginTx()(implicit timeout: Timeout): Future[Unit] = traced {
    argsNotNull()
    simpleQueryIgnoreResult(NativeSql("BEGIN"))
  }

  override def commitTx()(implicit timeout: Timeout): Future[Unit] = traced {
    argsNotNull()
    simpleQueryIgnoreResult(NativeSql("COMMIT"))
  }

  override def rollbackTx()(implicit timeout: Timeout): Future[Unit] = traced {
    argsNotNull()
    simpleQueryIgnoreResult(NativeSql("ROLLBACK"))
  }

  override def validate()(implicit timeout: Timeout): Future[Unit] = traced {
    argsNotNull()
    simpleQueryIgnoreResult(NativeSql("")).recoverWith {
      case ex: IllegalSessionStateException => Future.failed(ex)
      case NonFatal(ex) => Future.failed(new ConnectionValidationException(ex))
    }
  }

  override def release(): Future[Unit] = traced {
    //TODO do nothing if already released
    fsmManager.ifReadyF { (_, _) =>
      logger.debug(s"Releasing connection on client request")
      doRelease("Connection released by client")
    }
  }

  override def forceRelease(): Future[Unit] = traced {
    //TODO do nothing if already released
    logger.debug("Forcing a connection release")
    doRelease("Connection released by client (forced)")
  }

  def init(dbUser: String, dbName: String, authenticator: Authenticator): Future[Unit] = traced {
    argsNotNull()
    logger.debug(s"Initializing connection")
    val initPromise = Promise[BackendKeyData]
    fsmManager.triggerTransition(State.authenticating(initPromise, authenticator)(out))

    out.writeAndFlush(Startup(dbUser, dbName)).recoverWith(writeFailureHandler).flatMap { _ =>
      initPromise.future.map { returnedBkd =>
        maybeBackendKeyData = Some(returnedBkd)
        ()
      }
    }
  }

  protected def handleClientCharsetChange(charset: Charset): Unit

  protected def handleServerCharsetChange(charset: Charset): Unit

  protected final def handleBackendMessage(msg: PgBackendMessage): Unit = traced {
    //argsNotNull()
    logger.trace(s"Handling backend message $msg")
    msg match {
      case paramStatus: ParameterStatus => handleParamStatusChange(paramStatus)
      case _ =>
        fsmManager.currentState.onMessage(msg) match {
          case Stay(afterAcknowledged) => afterAcknowledged.foreach(_.apply())
          case Goto(newState, afterTransitionAction) => fsmManager.triggerTransition(newState, afterTransitionAction)
          case Fatal(ex, afterReleaseAction) =>
            logger.error("Fatal error occurred, connection will be closed", ex)
            doRelease(ex).map(_ => afterReleaseAction.foreach(_.apply()))
        }
    }
    ()
  }

  override protected[core] final def handleFatalError(msg: String, cause: Throwable): Unit = traced {
    argsNotNull()
    checkNonEmptyString(msg)
    logger.error(msg, cause)
    doRelease(cause)
    ()
  }

  override private[core]
  def statementStream(nativeSql: NativeSql, params: Vector[Argument])
                     (implicit timeout: Timeout): RowPublisher = {
    fsmManager.ifReady { (reqId, txStatus) =>

      val parseAndBind = newParseAndBind(nativeSql, params)

      val preparePortalFun = (publisher: PgRowPublisher) => {
        val msgs@ParseAndBind(parse, _) = parseAndBind
        val describePromise = Promise[PortalDescData]
        val parsePromise = Promise[Unit]

        writeInitialExecuteMessages(txStatus, msgs, describePromise, parsePromise, publisher)
          .recoverWith(writeFailureHandler)
          .flatMap(_ => updateStmtCacheIfNeeded(parse, parsePromise.future, nativeSql))
          .flatMap(_ => describePromise.future)
      }

      val publisher = new PgRowPublisher(
        preparePortal = preparePortalFun,
        portalName = parseAndBind.bind.portal,
        pgTypes = config.pgTypes,
        typeConverters = config.typeConverters,
        sessionParams = sessionParams,
        maybeTimeoutHandler = newTimeoutHandler(reqId, timeout),
        fatalErrorNotifier = handleFatalError
      )(reqId, out, ec)

      fsmManager.triggerTransition(State.Streaming.waitingForSubscriber)

      publisher
    }
  }

  private def updateStmtCacheIfNeeded(maybeParse: Option[Parse],
                                      parseFut: Future[Unit],
                                      nativeSql: NativeSql): Future[Unit] = {
    maybeStmtCache.fold(Future.unit) { stmtCache =>
      maybeParse.flatMap(_.optionalName) match {
        case Some(stmtName) => parseFut.map { _ =>
          val (newCache, evicted) = stmtCache.put(nativeSql, stmtName)
          //TODO close evicted
          maybeStmtCache = Some(newCache)
        }
        case None => Future.unit
      }
    }
  }

  private def writeInitialExecuteMessages(txStatus: TxStatus,
                                          messages: ParseAndBind,
                                          describePromise: Promise[PortalDescData],
                                          parsePromise: Promise[Unit],
                                          publisher: PgRowPublisher): Future[Unit] = {
    txStatus match {
      case TxStatus.Active | TxStatus.Failed =>
        fsmManager.triggerTransition(
          waitingForDescribeResult(messages.bind.portal, describePromise, parsePromise, publisher)
        )
        messages.parse.foreach(out.write(_))
        out.writeAndFlush(messages.bind, DescribePortal(messages.bind.portal), Sync)

      case TxStatus.Idle =>
        fsmManager.triggerTransition(
          beginningTx(messages.parse, messages.bind, describePromise, parsePromise, publisher)
        )
        out.writeAndFlush(Query(NativeSql("BEGIN")))
    }
  }

  private def waitingForDescribeResult(portalName: Option[PortalName],
                                       describePromise: Promise[PortalDescData],
                                       parsePromise: Promise[Unit],
                                       publisher: PgRowPublisher): StrmWaitingForDescribe = {
    State.Streaming.waitingForDescribe(
      txMgmt = false,
      publisher = publisher,
      portalName = portalName,
      describePromise = describePromise,
      parsePromise = parsePromise)(out, ec)
  }

  private def beginningTx(parse: Option[Parse],
                          bind: Bind,
                          describePromise: Promise[PortalDescData],
                          parsePromise: Promise[Unit],
                          publisher: PgRowPublisher): StrmBeginningTx = {
    State.Streaming.beginningTx(
      maybeParse = parse,
      bind = bind,
      publisher = publisher,
      describePromise = describePromise,
      parsePromise = parsePromise)(out, ec)
  }

  sealed trait StatementStatus

  object StatementStatus {

    case class NotCachedDoCache(stmtName: StmtName) extends StatementStatus

    case object NotCachedDontCache extends StatementStatus

    case class Cached(stmtName: StmtName) extends StatementStatus

  }

  private def determineStmtStatus(nativeSql: NativeSql): StatementStatus = {
    maybeStmtCache.fold[StatementStatus](StatementStatus.NotCachedDontCache) {
      stmtCache =>
        stmtCache.get(nativeSql) match {
          case (newCache, Some(stmtName)) =>
            maybeStmtCache = Some(newCache)
            StatementStatus.Cached(stmtName)

          case (newCache, None) =>
            maybeStmtCache = Some(newCache)
            if (shouldCache(nativeSql)) StatementStatus.NotCachedDoCache(nextStmtName())
            else StatementStatus.NotCachedDontCache
        }
    }
  }

  object ParseAndBind {
    def apply(bind: Bind): ParseAndBind = ParseAndBind(None, bind)

    def apply(parse: Parse, bind: Bind): ParseAndBind = ParseAndBind(Some(parse), bind)
  }

  case class ParseAndBind(parse: Option[Parse], bind: Bind)

  private def newParseAndBind(nativeSql: NativeSql, params: Vector[Argument]): ParseAndBind = {
    def newParse(maybeStmtName: Option[StmtName]): Parse = {
      Parse(maybeStmtName, nativeSql, params.map(_.dataTypeOid))
    }

    def newBind(maybeStmtName: Option[StmtName]): Bind = {
      Bind(portal = None, maybeStmtName, params, ReturnColFormats.AllBinary)
    }

    determineStmtStatus(nativeSql) match {
      case StatementStatus.Cached(stmtName) => ParseAndBind(newBind(Some(stmtName)))
      case StatementStatus.NotCachedDoCache(stmtName) => ParseAndBind(newParse(Some(stmtName)), newBind(Some(stmtName)))
      case StatementStatus.NotCachedDontCache => ParseAndBind(newParse(None), newBind(None))
    }
  }

  private def shouldCache(nativeSql: NativeSql): Boolean = {
    //for now, all statements are cached if cache is enabled
    true
  }

  override private[core] def executeStatementForRowsAffected(nativeSql: NativeSql,
                                                             params: Vector[Argument])(
                                                              implicit timeout: Timeout): Future[Long] = traced {
    fsmManager.ifReadyF { (reqId, _) =>
      logger.debug(s"Executing write-only statement '$nativeSql'")

      val ParseAndBind(parse, bind) = newParseAndBind(nativeSql, params)

      val parsePromise = Promise[Unit]
      val resultPromise = Promise[Long]

      fsmManager.triggerTransition(State.executingWriteOnly(parsePromise, resultPromise))
      parse.foreach(out.write(_))
      out
        .writeAndFlush(bind, Execute(optionalPortalName = bind.portal, optionalFetchSize = None), Sync)
        .recoverWith(writeFailureHandler)
        .flatMap { _ =>
          val timeoutTask = newTimeoutHandler(reqId, timeout).map(_.scheduleTimeoutTask(reqId))
          updateStmtCacheIfNeeded(parse, parsePromise.future, nativeSql)
            .flatMap(_ => resultPromise.future)
            .andThen { case _ =>
              timeoutTask.foreach(_.cancel())
            }
        }
    }
  }

  private[core]
  def subscribeToStatementArgsStream[A](nativeStatement: PgNativeStatement,
                                        argsSource: Publisher[A],
                                        argsConverter: A => Try[Vector[Argument]]): Future[Unit] = {
    fsmManager.ifReadyF { (_, txStatus) =>
      val subscriber = new StatementArgsSubscriber(
        nativeStmt = nativeStatement,
        bufferCapacity = config.subscriberBufferCapacity,
        minDemandRequest = config.subscriberMinDemandRequestSize,
        initialTxStatus = txStatus,
        batchExecutor = this,
        argConverter = argsConverter
      )
      argsSource.subscribe(subscriber)
      subscriber.done.map(_ => ())
    }
  }

  private def batchMessages(nativeStmt: PgNativeStatement,
                            batch: Vector[Vector[Argument]],
                            first: Boolean): Vector[PgFrontendMessage] = {
    val execute = Execute(optionalPortalName = None, optionalFetchSize = None)
    val parseVec = {
      if (first) {
        Vector(Parse(None, nativeStmt.sql, batch.head.map(_.dataTypeOid))) //TODO guard against an empty batch?
      } else Vector.empty
    }

    parseVec ++ batch.flatMap { params =>
      Vector(Bind(execute.optionalPortalName, None, params, ReturnColFormats.AllBinary), execute)
    } :+ Sync
  }

  override private[core]
  def executeBatch(nativeStmt: PgNativeStatement,
                   batch: Vector[Vector[Argument]],
                   first: Boolean): Future[TxStatus] = {
    val batchPromise = Promise[TxStatus]
    fsmManager.triggerTransition(State.executingBatch(batchPromise))
    val batchMsgs = batchMessages(nativeStmt, batch, first)
    out
      .writeAndFlush(batchMsgs)
      .recoverWith(writeFailureHandler)
      .flatMap(_ => batchPromise.future)
  }

  override private[core]
  def completeBatch(txStatus: TxStatus): Unit = {
    fsmManager.triggerTransition(Idle(txStatus))
  }

  override private[core] def handleWriteError(cause: Throwable): Unit = traced {
    handleFatalError("Write error occurred, the connection will be closed", cause)
  }

  private def simpleQueryIgnoreResult(sql: NativeSql)(implicit timeout: Timeout): Future[Unit] = traced {
    fsmManager.ifReadyF {
      (reqId, _) =>
        val queryPromise = Promise[Unit]
        fsmManager.triggerTransition(State.simpleQuerying(queryPromise))
        out
          .writeAndFlush(Query(sql))
          .recoverWith(writeFailureHandler)
          .map(_ => newTimeoutHandler(reqId, timeout).map(_.scheduleTimeoutTask(reqId)))
          .flatMap {
            maybeTimeoutTask =>
              queryPromise.future.andThen {
                case _ =>
                  maybeTimeoutTask.foreach(_.cancel())
              }
          }
    }
  }

  private def handleCharsetChange(pgCharsetName: String)(consumer: Charset => Unit): Unit = traced {
    try {
      consumer(PgCharset.toJavaCharset(pgCharsetName))
    } catch {
      case ex: PgUnsupportedCharsetException => handleFatalError(ex.getMessage, ex)
    }
  }

  private def handleParamStatusChange(p: ParameterStatus): Unit = traced {
    p match {
      case ParameterStatus(SessionParamKey("client_encoding"), SessionParamVal(pgCharsetName)) =>
        handleCharsetChange(pgCharsetName) {
          charset =>
            handleClientCharsetChange(charset)
            sessionParams = sessionParams.copy(clientCharset = charset)
        }

      case ParameterStatus(SessionParamKey("server_encoding"), SessionParamVal(pgCharsetName)) =>
        handleCharsetChange(pgCharsetName) {
          charset =>
            handleServerCharsetChange(charset)
            sessionParams = sessionParams.copy(serverCharset = charset)
        }

      case _ => ()
    }
    logger.debug(s"Session parameter '${
      p.key.value
    }' is now set to '${
      p.value.value
    }'")
  }

  private def nextStmtName(): StmtName = traced {
    StmtName("S" + stmtCounter.incrementAndGet())
  }

  private def doRelease(cause: Throwable): Future[Unit] = traced {
    out
      .writeAndFlush(Terminate)
      .recover {
        case writeEx =>
          logger.error("Write error occurred when terminating connection", writeEx)
      }
      .flatMap {
        _ =>
          val connClosedEx = cause match {
            case ex: ConnectionClosedException => ex
            case ex => new ConnectionClosedException("Connection closed", ex)
          }
          fsmManager.triggerTransition(ConnectionClosed(connClosedEx))
          out.close().recover {
            case closeEx =>
              logger.error("Channel close error occurred when terminating connection", closeEx)
          }
      }
  }

  private def doRelease(cause: String): Future[Unit] = traced {
    doRelease(new ConnectionClosedException(cause))
  }

  private def writeFailureHandler[T]: PartialFunction[Throwable, Future[T]] = {
    case writeEx =>
      handleWriteError(writeEx)
      Future.failed(writeEx)
  }

  private def newTimeoutHandler(reqId: RequestId,
                                timeout: Timeout): Option[TimeoutHandler] = traced {
    if (timeout.value.isFinite()) {
      val duration = FiniteDuration(timeout.value.length, timeout.value.unit)
      Some(new TimeoutHandler(scheduler, duration, timeoutAction = () => {
        val shouldCancel = fsmManager.startHandlingTimeout(reqId)
        if (shouldCancel) {
          logger.debug(s"Timeout occurred for request '$reqId', cancelling it")
          maybeBackendKeyData.foreach {
            bkd =>
              requestCanceler(bkd).onComplete(_ => fsmManager.finishHandlingTimeout())
          }
        } else {
          logger.debug(s"Timeout task ran for request '$reqId', but this request is not being executed anymore")
        }
      }))
    } else {
      None
    }
  }

  private[core] def deallocateStatement(nativeSql: NativeSql): Future[Unit] = traced {
    maybeStmtCache.fold(Future.unit) {
      stmtCache =>
        fsmManager.ifReadyF {
          (_, txStatus) =>
            stmtCache.evict(nativeSql) match {
              case Some((newCache, evictedName)) =>
                maybeStmtCache = Some(newCache)
                deallocateCached(evictedName)

              case None =>
                fsmManager.triggerTransition(Idle(txStatus))
                Future.unit
            }
        }
    }
  }

  private def deallocateCached(stmtName: StmtName): Future[Unit] = traced {
    val promise = Promise[Unit]
    fsmManager.triggerTransition(new DeallocatingStatement(promise))
    out
      .writeAndFlush(CloseStatement(Some(stmtName)), Sync)
      .recoverWith {
        case writeEx =>
          handleWriteError(writeEx)
          Future.failed(writeEx)
      }
      .flatMap(_ => promise.future)
  }
}
