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

package io.rdbc.pgsql.transport.netty

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import io.netty.bootstrap.Bootstrap
import io.netty.channel._
import io.netty.channel.group.DefaultChannelGroup
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.timeout.WriteTimeoutHandler
import io.netty.util.concurrent.GlobalEventExecutor
import io.rdbc.api.exceptions.RdbcException
import io.rdbc.implbase.ConnectionFactoryPartialImpl
import io.rdbc.pgsql.core.exception.{PgDriverInternalErrorException, PgUncategorizedException}
import io.rdbc.pgsql.core.pgstruct.messages.backend.BackendKeyData
import io.rdbc.pgsql.core.pgstruct.messages.frontend.{CancelRequest, Terminate}
import io.rdbc.pgsql.core.types.PgTypeRegistry
import io.rdbc.pgsql.core.{AbstractPgConnection, ConnId, PgConnectionConfig}
import io.rdbc.sapi.{Timeout, TypeConverterRegistry}
import io.rdbc.util.Logging
import io.rdbc.util.scheduler.JdkScheduler

import scala.concurrent.Future
import scala.util.control.NonFatal

object NettyPgConnectionFactory extends Logging {

  def apply(config: NettyPgConnFactoryConfig): NettyPgConnectionFactory = {
    new NettyPgConnectionFactory(config)
  }

  def apply(host: String,
            port: Int,
            username: String,
            password: String): NettyPgConnectionFactory = {
    apply(
      NettyPgConnFactoryConfig(host, port, username, password)
    )
  }

}

class NettyPgConnectionFactory protected(val config: NettyPgConnFactoryConfig)
  extends ConnectionFactoryPartialImpl
    with Logging {

  protected implicit val ec = config.ec

  private[this] val pgTypes = {
    PgTypeRegistry(config.pgTypesProviders.flatMap(_.types))
  }

  private[this] val typeConverters = {
    TypeConverterRegistry {
      config.typeConvertersProviders.flatMap(_.typeConverters)
    }
  }

  private[this] val scheduler = {
    new JdkScheduler(config.eventLoopGroup)
  }

  private[this] val openChannels = {
    new DefaultChannelGroup(GlobalEventExecutor.INSTANCE) //TODO really global?
  }

  private[this] val actorSystem = {
    ActorSystem("rdbc-pgsql-netty", config.actorSystemConfig)
  }

  private[this] val streamMaterializer = {
    ActorMaterializer(config.actorMaterializerSettings)(actorSystem)
  }

  private[this] val shutDown = new AtomicBoolean(false)

  private class ConnChannelInitializer extends ChannelInitializer[Channel] {
    @volatile private var _maybeConn = Option.empty[NettyPgConnection]

    def maybeConn: Option[NettyPgConnection] = _maybeConn

    def initChannel(ch: Channel): Unit = {
      val decoderHandler = new PgMsgDecoderHandler(config.msgDecoderFactory)
      val encoderHandler = new PgMsgEncoderHandler(config.msgEncoderFactory)

      ch.pipeline().addLast(framingHandler)
      ch.pipeline().addLast(decoderHandler)
      ch.pipeline().addLast(encoderHandler)
      ch.pipeline().addLast(new WriteTimeoutHandler(config.writeTimeout.toSeconds.toInt))

      val conn = pgConnection(ch, decoderHandler, encoderHandler)
      ch.pipeline().addLast(conn.handler)

      _maybeConn = Some(conn)
    }

    override def channelActive(ctx: ChannelHandlerContext): Unit = {
      openChannels.add(ctx.channel())
      super.channelActive(ctx)
    }
  }

  def connection()(implicit timeout: Timeout): Future[AbstractPgConnection] = traced {
    if (!shutDown.get()) {
      val initializer = new ConnChannelInitializer
      baseBootstrap(Some(timeout))
        .handler(initializer)
        .connect().scalaFut
        .flatMap { _ =>
          val conn = initializer.maybeConn.getOrElse(throw new PgDriverInternalErrorException(
            "Channel initializer did not create a connection instance"
          ))
          conn.init(config.dbRole, config.dbName, config.authenticator).map(_ => conn)
        }
        .recoverWith {
          case ex: RdbcException => Future.failed(ex)
          case NonFatal(ex) =>
            Future.failed(new PgUncategorizedException(ex.getMessage, ex))
        }
    } else {
      Future.failed(new PgUncategorizedException("The factory is shut down"))
    }
  }

  def shutdown(): Future[Unit] = traced {
    if (shutDown.compareAndSet(false, true)) {
      def warn(detail: String): PartialFunction[Throwable, Unit] = {
        case NonFatal(ex) =>
          logger.warn(s"Error occurred during connection factory shutdown: $detail", ex)
      }

      for {
        _ <- openChannels.writeAndFlush(Terminate).scalaFut
          .recover(warn("could not write 'Terminate' message"))

        _ <- openChannels.close().scalaFut
          .recover(warn("could not close open channels"))

        _ <- config.eventLoopGroup.shutdownGracefully(0L, 0L, TimeUnit.SECONDS).scalaFut
          .recover(warn("could not shutdown event loop group"))

        _ <- actorSystem.terminate()
          .recover(warn("could not terminate the actor system"))
      } yield ()
    } else {
      logger.warn("Shutdown request received for already shut down connection factory")
      Future.successful(())
    }
  }

  private def pgConnection(ch: Channel,
                           decoderHandler: PgMsgDecoderHandler,
                           encoderHandler: PgMsgEncoderHandler): NettyPgConnection = traced {
    val connConfig = PgConnectionConfig(
      pgTypes = pgTypes,
      typeConverters = typeConverters,
      maxBatchSize = config.maxBatchSize,
      stmtCacheConfig = config.stmtCacheConfig
    )

    new NettyPgConnection(
      id = ConnId(ch.id().asShortText()),
      config = connConfig,
      out = new NettyChannelWriter(ch),
      decoder = decoderHandler,
      encoder = encoderHandler,
      ec = ec,
      scheduler = scheduler,
      requestCanceler = abortRequest,
      streamMaterializer = streamMaterializer
    )
  }

  private def baseBootstrap(connectTimeout: Option[Timeout]): Bootstrap = traced {
    val address = if (config.address.isUnresolved) {
      new InetSocketAddress(config.address.getHostString, config.address.getPort)
    } else {
      config.address
    }

    val bootstrap = new Bootstrap()
      .group(config.eventLoopGroup)
      .channelFactory(config.channelFactory)
      .remoteAddress(address)

    config.channelOptions.foreach { opt =>
      bootstrap.option(opt.option.asInstanceOf[ChannelOption[Any]], opt.value)
    }
    connectTimeout.foreach { timeout =>
      if (timeout.value.isFinite()) {
        bootstrap.option[Integer](
          ChannelOption.CONNECT_TIMEOUT_MILLIS,
          timeout.value.toMillis.toInt
        )
      }
    }

    bootstrap
  }

  private def abortRequest(bkd: BackendKeyData): Future[Unit] = traced {
    baseBootstrap(connectTimeout = None)
      .handler {
        channelInitializer { ch =>
          ch.pipeline().addLast(new PgMsgEncoderHandler(config.msgEncoderFactory))
        }
      }
      .connect().scalaFut
      .flatMap { channel =>
        channel
          .writeAndFlush(CancelRequest(bkd.pid, bkd.key)).scalaFut
          .flatMap(_ => channel.close().scalaFut)
          .map(_ => ())
      }
      .recoverWith {
        case NonFatal(ex) => Future.failed(new PgUncategorizedException("Could not abort request", ex))
      }
  }

  private def framingHandler: LengthFieldBasedFrameDecoder = {
    // format: off
    val lengthFieldLength = 4
    new LengthFieldBasedFrameDecoder(
      /* max frame length       = */ Int.MaxValue,
      /* length field offset    = */ 1,
      /* length field length    = */ lengthFieldLength,
      /* length adjustment      = */ -1 * lengthFieldLength,
      /* initial bytes to strip = */ 0
    )
    // format: on
  }


  /* Scala 2.11 compat */
  private def channelInitializer(f: Channel => Unit): ChannelInitializer[Channel] = {
    new ChannelInitializer[Channel] {
      def initChannel(ch: Channel): Unit = f(ch)
    }
  }
}
