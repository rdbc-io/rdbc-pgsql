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

import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.ScheduledFuture

import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelOption.SO_KEEPALIVE
import io.netty.channel._
import io.netty.channel.epoll.{Epoll, EpollEventLoopGroup, EpollSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.timeout.WriteTimeoutHandler
import io.rdbc.api.exceptions.ConnectException
import io.rdbc.api.exceptions.ConnectException.UncategorizedConnectException
import io.rdbc.pgsql.core.auth.{Authenticator, UsernamePasswordAuthenticator}
import io.rdbc.pgsql.core.codec.{DecoderFactory, EncoderFactory}
import io.rdbc.pgsql.core.messages.backend.BackendKeyData
import io.rdbc.pgsql.core.messages.frontend.CancelRequest
import io.rdbc.pgsql.core.types.PgTypeRegistry
import io.rdbc.pgsql.netty.codec.{NettyPgMsgDecoder, NettyPgMsgEncoder}
import io.rdbc.pgsql.scodec.types.ScodecBuiltInTypes
import io.rdbc.pgsql.scodec.{ScodecDecoderFactory, ScodecEncoderFactory}
import io.rdbc.sapi.{ConnectionFactory, TypeConverterRegistry}
import io.rdbc.{ImmutSeq, typeconv}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

case class ChannelOptionValue[T](option: ChannelOption[T], value: T)

class EventLoopGroupExecutionContext(eventLoopGroup: EventLoopGroup) extends ExecutionContext {

  private val fallbackEc = ExecutionContext.global

  override def execute(runnable: Runnable): Unit = {
    if (!eventLoopGroup.isShutdown) {
      eventLoopGroup.execute(runnable)
    } else {
      fallbackEc.execute(runnable)
    }
  }
  override def reportFailure(cause: Throwable): Unit = cause.printStackTrace()
}

class ScheduledTask(scheduledFuture: ScheduledFuture[_]) {
  def cancel(): Unit = {
    scheduledFuture.cancel(false)
  }
}

trait TaskScheduler {
  def schedule(delay: FiniteDuration)(action: => Unit): ScheduledTask
}

class EventLoopGroupScheduler(eventLoopGroup: EventLoopGroup) extends TaskScheduler {
  def schedule(delay: FiniteDuration)(action: => Unit): ScheduledTask = {
    val fut = eventLoopGroup.schedule(new Runnable() {
      def run() = action
    }, delay.length, delay.unit)
    new ScheduledTask(fut)
  }
}

object PgNettyConnectionFactory {
  def apply(host: String, port: Int, username: String, password: String, database: String): PgNettyConnectionFactory = {
    new PgNettyConnectionFactory(
      remoteAddr = InetSocketAddress.createUnresolved(host, port),
      dbUser = username,
      dbName = database,
      authenticator = new UsernamePasswordAuthenticator(username, password),
      rdbcTypeConvRegistry = typeconv.BuiltInConverters,
      pgTypeConvRegistry = ScodecBuiltInTypes,
      msgDecoderFactory = new ScodecDecoderFactory,
      msgEncoderFactory = new ScodecEncoderFactory,
      writeTimeout = 10.seconds,
      channelFactory = defaultChannelFactory,
      eventLoopGroup = defaultEventLoopGroup,
      channelOptions = Vector(ChannelOptionValue[java.lang.Boolean](SO_KEEPALIVE, true))) //TODO
  }

  private val defaultChannelFactory: ChannelFactory[SocketChannel] = {
    if (Epoll.isAvailable) new ReflectiveChannelFactory(classOf[EpollSocketChannel])
    else new ReflectiveChannelFactory(classOf[NioSocketChannel])
  }

  private val defaultEventLoopGroup: EventLoopGroup = {
    if (Epoll.isAvailable) new EpollEventLoopGroup()
    else new NioEventLoopGroup()
  }
}

class PgNettyConnectionFactory protected(remoteAddr: SocketAddress,
                                         dbUser: String,
                                         dbName: String,
                                         authenticator: Authenticator,
                                         rdbcTypeConvRegistry: TypeConverterRegistry,
                                         pgTypeConvRegistry: PgTypeRegistry,
                                         msgDecoderFactory: DecoderFactory,
                                         msgEncoderFactory: EncoderFactory,
                                         writeTimeout: FiniteDuration,
                                         channelFactory: ChannelFactory[_ <: Channel],
                                         eventLoopGroup: EventLoopGroup,
                                         channelOptions: ImmutSeq[ChannelOptionValue[_]])
  extends ConnectionFactory {

  thisFactory =>

  val typeConverterRegistry = rdbcTypeConvRegistry
  private val scheduler = new EventLoopGroupScheduler(eventLoopGroup)
  private implicit val ec = new EventLoopGroupExecutionContext(eventLoopGroup) //TODO are you sure?

  def connection(): Future[PgNettyConnection] = {

    var conn: PgNettyConnection = null
    //TODO bossGroup, workerGroup - familarize yourself
    val bootstrap = new Bootstrap()
      .group(eventLoopGroup)
      .channelFactory(channelFactory)
      .remoteAddress(remoteAddr)
      .handler(new ChannelInitializer[Channel] {
        def initChannel(ch: Channel): Unit = {
          val decoderHandler = new NettyPgMsgDecoder(msgDecoderFactory.decoder())
          val encoderHandler = new NettyPgMsgEncoder(msgEncoderFactory.encoder())
          conn = new PgNettyConnection(pgTypeConvRegistry, rdbcTypeConvRegistry, new ChannelWriter(ch), decoderHandler, encoderHandler, ec, scheduler, thisFactory.abortRequest)

          ch.pipeline().addLast(framingHandler)
          ch.pipeline().addLast(decoderHandler)
          ch.pipeline().addLast(encoderHandler)
          ch.pipeline().addLast(new WriteTimeoutHandler(writeTimeout.toSeconds.toInt))
          ch.pipeline().addLast(conn)
        }
      })

    channelOptions.foreach(opt => bootstrap.option(opt.option.asInstanceOf[ChannelOption[Any]], opt.value))

    val connectionFut = bootstrap.connect().scalaFut.flatMap { _ =>
      conn.init(dbUser, dbName, authenticator).map(_ => conn)
    }.recoverWith {
      case ex: ConnectException => Future.failed(ex)
      case NonFatal(ex) => Future.failed(UncategorizedConnectException(ex.getMessage)) //TODO cause
    }

    connectionFut.onFailure {
      case _ => conn.release()
    }

    connectionFut
  }

  def abortRequest(bkd: BackendKeyData): Future[Unit] = {
    //TODO code dupl
    val bootstrap = new Bootstrap()
      .group(eventLoopGroup)
      .channelFactory(channelFactory)
      .remoteAddress(remoteAddr)
      .handler(new ChannelInitializer[Channel] {
        def initChannel(ch: Channel): Unit = {
          ch.pipeline().addLast(new NettyPgMsgEncoder(msgEncoderFactory.encoder()))
        }
      })

    channelOptions.foreach(opt => bootstrap.option(opt.option.asInstanceOf[ChannelOption[Any]], opt.value))

    val connectFut = bootstrap.connect()
    connectFut.scalaFut.flatMap { _ =>
      connectFut.channel().writeAndFlush(CancelRequest(bkd.pid, bkd.key)).scalaFut.flatMap(_ =>
        connectFut.channel().close().scalaFut
      )
    }.recoverWith {
      case NonFatal(ex) => Future.failed(ex) //TODO cause
    }
  }

  private val framingHandler: LengthFieldBasedFrameDecoder = {
    val lengthFieldLength = 4
    new LengthFieldBasedFrameDecoder(
      Int.MaxValue, /* max frame length */
      1, /* length field offset */
      lengthFieldLength,
      -1 * lengthFieldLength, /* length adjustment */
      0 /* initial bytes to strip */
    )
  }

  def shutdown(): Future[Unit] = {
    //TODO shutdown gracefully?
    //TODO cancel all connections?
    val x = eventLoopGroup.shutdownGracefully().scalaFut
    x.map(_ => ())(ec)
  }
}
