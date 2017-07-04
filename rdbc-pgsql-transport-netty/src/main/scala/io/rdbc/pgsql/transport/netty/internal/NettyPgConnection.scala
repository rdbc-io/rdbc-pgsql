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

package io.rdbc.pgsql.transport.netty.internal

import java.nio.charset.Charset

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.rdbc.pgsql.core._
import io.rdbc.pgsql.core.pgstruct.messages.backend._
import io.rdbc.util.scheduler.TaskScheduler

import scala.concurrent.ExecutionContext

private[netty] class NettyPgConnection(id: ConnId,
                                       config: PgConnectionConfig,
                                       out: ChannelWriter,
                                       decoder: PgMsgDecoderHandler,
                                       encoder: PgMsgEncoderHandler,
                                       ec: ExecutionContext,
                                       scheduler: TaskScheduler,
                                       requestCanceler: RequestCanceler)
  extends AbstractPgConnection(
    id = id,
    config = config,
    out = out,
    ec = ec,
    scheduler = scheduler,
    requestCanceler = requestCanceler) {

  val handler = new SimpleChannelInboundHandler[PgBackendMessage] {
    def channelRead0(ctx: ChannelHandlerContext, msg: PgBackendMessage): Unit = {
      handleBackendMessage(msg)
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
      handleFatalError("Unhandled exception occurred in channel handler", cause)
    }
  }

  protected def handleClientCharsetChange(charset: Charset): Unit = {
    encoder.changeCharset(charset)
  }

  protected def handleServerCharsetChange(charset: Charset): Unit = {
    decoder.changeCharset(charset)
  }
}
