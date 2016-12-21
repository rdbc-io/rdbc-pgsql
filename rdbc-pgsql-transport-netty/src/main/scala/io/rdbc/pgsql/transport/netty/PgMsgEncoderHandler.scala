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

package io.rdbc.pgsql.transport.netty

import java.nio.charset.Charset

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder
import io.rdbc.pgsql.core.SessionParams
import io.rdbc.pgsql.core.codec.{Encoder, EncoderFactory}
import io.rdbc.pgsql.core.pgstruct.messages.frontend.PgFrontendMessage
import io.rdbc.util.Logging

private[netty] class PgMsgEncoderHandler(encoderFactory: EncoderFactory)
    extends MessageToByteEncoder[PgFrontendMessage]
    with Logging {

  @volatile private[this] var encoder: Encoder = {
    encoderFactory.encoder(SessionParams.default.clientCharset)
  }

  def encode(ctx: ChannelHandlerContext, msg: PgFrontendMessage, out: ByteBuf): Unit = {
    out.writeBytes(encoder.encode(msg).toArray)
  }

  def changeCharset(charset: Charset): Unit = {
    logger.debug(s"Message encoder charset changed to '$charset'")
    encoder = encoderFactory.encoder(charset)
  }

  //TODO should this override exceptionCaught?
}
