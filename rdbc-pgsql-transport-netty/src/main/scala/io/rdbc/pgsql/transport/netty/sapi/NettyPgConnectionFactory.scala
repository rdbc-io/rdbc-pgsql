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

package io.rdbc.pgsql.transport.netty.sapi

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import io.netty.bootstrap.Bootstrap
import io.netty.channel._
import io.netty.channel.group.DefaultChannelGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.timeout.WriteTimeoutHandler
import io.netty.util.concurrent.GlobalEventExecutor
import io.rdbc.ImmutSeq
import io.rdbc.sapi.exceptions.RdbcException
import io.rdbc.implbase.ConnectionFactoryPartialImpl
import io.rdbc.pgsql.core._
import io.rdbc.pgsql.core.auth.Authenticator
import io.rdbc.pgsql.core.codec.{DecoderFactory, EncoderFactory}
import io.rdbc.pgsql.core.config.sapi.{PgConnFactoryConfig, StmtCacheConfig}
import io.rdbc.pgsql.core.exception.{PgDriverInternalErrorException, PgUncategorizedException}
import io.rdbc.pgsql.core.pgstruct.messages.backend.BackendKeyData
import io.rdbc.pgsql.core.pgstruct.messages.frontend.{CancelRequest, Terminate}
import io.rdbc.pgsql.core.types.{PgTypeRegistry, PgTypesProvider}
import io.rdbc.pgsql.transport.netty.internal._
import io.rdbc.pgsql.transport.netty.internal.Compat._
import io.rdbc.sapi.{Timeout, TypeConverterRegistry, TypeConvertersProvider}
import io.rdbc.util.Logging
import io.rdbc.util.scheduler.JdkScheduler

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

class NettyPgConnectionFactory protected(val nettyConfig: NettyPgConnectionFactory.Config)
  extends ConnectionFactoryPartialImpl
    with PgConnectionFactory
    with Logging {

  val pgConfig: PgConnFactoryConfig = nettyConfig.pgConfig

  protected implicit val ec: ExecutionContext = pgConfig.ec

  private[this] val pgTypes = {
    PgTypeRegistry(pgConfig.pgTypesProviders.flatMap(_.types))
  }

  private[this] val typeConverters = {
    TypeConverterRegistry {
      pgConfig.typeConvertersProviders.flatMap(_.typeConverters)
    }
  }

  private[this] val scheduler = {
    new JdkScheduler(nettyConfig.eventLoopGroup)
  }

  private[this] val openChannels = {
    new DefaultChannelGroup(GlobalEventExecutor.INSTANCE) //TODO really global?
  }

  private[this] val shutDown = new AtomicBoolean(false)

  private class ConnChannelInitializer extends ChannelInitializer[Channel] {
    @volatile private var _maybeConn = Option.empty[NettyPgConnection]

    def maybeConn: Option[NettyPgConnection] = _maybeConn

    def initChannel(ch: Channel): Unit = {
      val decoderHandler = new PgMsgDecoderHandler(pgConfig.msgDecoderFactory)
      val encoderHandler = new PgMsgEncoderHandler(pgConfig.msgEncoderFactory)

      ch.pipeline().addLast(framingHandler)
      ch.pipeline().addLast(decoderHandler)
      ch.pipeline().addLast(encoderHandler)
      if (pgConfig.writeTimeout.value.isFinite()) {
        ch.pipeline().addLast(new WriteTimeoutHandler(pgConfig.writeTimeout.value.toSeconds.toInt))
      }

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
          initializer.maybeConn.map(Future.successful).getOrElse(Future.failed(new PgDriverInternalErrorException(
            "Channel initializer did not create a connection instance"
          ))).flatMap { conn =>
            conn.init(pgConfig.dbName, pgConfig.authenticator).map(_ => conn)
          }
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

        _ <- nettyConfig.eventLoopGroup.shutdownGracefully(0L, 0L, TimeUnit.SECONDS).scalaFut
          .recover(warn("could not shutdown event loop group"))
      } yield ()
    } else {
      logger.warn("Shutdown request received for already shut down connection factory")
      Future.unit
    }
  }

  private def pgConnection(ch: Channel,
                           decoderHandler: PgMsgDecoderHandler,
                           encoderHandler: PgMsgEncoderHandler): NettyPgConnection = traced {
    val connConfig = PgConnectionConfig(
      pgTypes = pgTypes,
      typeConverters = typeConverters,
      subscriberMinDemandRequestSize = pgConfig.subscriberMinDemandRequestSize,
      subscriberBufferCapacity = pgConfig.subscriberBufferCapacity,
      stmtCacheConfig = pgConfig.stmtCacheConfig
    )

    new NettyPgConnection(
      id = ConnId(ch.id().asShortText()),
      config = connConfig,
      out = new NettyChannelWriter(ch),
      decoder = decoderHandler,
      encoder = encoderHandler,
      ec = ec,
      scheduler = scheduler,
      requestCanceler = abortRequest
    )
  }

  private def baseBootstrap(connectTimeout: Option[Timeout]): Bootstrap = traced {
    val address = new InetSocketAddress(pgConfig.host, pgConfig.port)
    val bootstrap = new Bootstrap()
      .group(nettyConfig.eventLoopGroup)
      .channelFactory(nettyConfig.channelFactory)
      .remoteAddress(address)

    nettyConfig.channelOptions.foreach { opt =>
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
          ch.pipeline().addLast(new PgMsgEncoderHandler(pgConfig.msgEncoderFactory))
          ()
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

object NettyPgConnectionFactory extends Logging {

  object Config {

    import PgConnFactoryConfig.{Defaults => PgDefaults}

    object Defaults {
      val channelFactory: ChannelFactory[_ <: Channel] = new NioChannelFactory

      def eventLoopGroup: EventLoopGroup = new NioEventLoopGroup

      val channelOptions: ImmutSeq[ChannelOptionValue[_]] = {
        Vector(ChannelOptionValue(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE))
      }
    }

    def apply(host: String,
              port: Int,
              authenticator: Authenticator,
              dbName: Option[String] = None,
              subscriberBufferCapacity: Int = PgDefaults.subscriberBufferCapacity,
              subscriberMinDemandRequestSize: Int = PgDefaults.subscriberMinDemandRequestSize,
              stmtCacheConfig: StmtCacheConfig = PgDefaults.stmtCacheConfig,
              typeConvertersProviders: ImmutSeq[TypeConvertersProvider] = PgDefaults.typeConvertersProviders,
              pgTypesProviders: ImmutSeq[PgTypesProvider] = PgDefaults.pgTypesProviders,
              msgDecoderFactory: DecoderFactory = PgDefaults.msgDecoderFactory,
              msgEncoderFactory: EncoderFactory = PgDefaults.msgEncoderFactory,
              writeTimeout: Timeout = PgDefaults.writeTimeout,
              ec: ExecutionContext = PgDefaults.ec,
              channelFactory: ChannelFactory[_ <: Channel] = Defaults.channelFactory,
              eventLoopGroup: EventLoopGroup = Defaults.eventLoopGroup,
              channelOptions: ImmutSeq[ChannelOptionValue[_]] = Defaults.channelOptions
             ): Config = {

      val pgConfig = PgConnFactoryConfig(
        host = host,
        port = port,
        dbName = dbName,
        authenticator = authenticator,
        typeConvertersProviders = typeConvertersProviders,
        pgTypesProviders = pgTypesProviders,
        subscriberBufferCapacity = subscriberBufferCapacity,
        subscriberMinDemandRequestSize = subscriberMinDemandRequestSize,
        stmtCacheConfig = stmtCacheConfig,
        msgDecoderFactory = msgDecoderFactory,
        msgEncoderFactory = msgEncoderFactory,
        writeTimeout = writeTimeout,
        ec = ec
      )

      Config(
        pgConfig = pgConfig,
        channelFactory = channelFactory,
        eventLoopGroup = eventLoopGroup,
        channelOptions = channelOptions
      )
    }
  }


  final case class Config(pgConfig: PgConnFactoryConfig,
                          channelFactory: ChannelFactory[_ <: Channel],
                          eventLoopGroup: EventLoopGroup,
                          channelOptions: ImmutSeq[ChannelOptionValue[_]])


  def apply(config: Config): NettyPgConnectionFactory = {
    new NettyPgConnectionFactory(config)
  }

}
