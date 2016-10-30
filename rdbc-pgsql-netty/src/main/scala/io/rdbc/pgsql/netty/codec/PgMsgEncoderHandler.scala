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

package io.rdbc.pgsql.netty.codec

import java.nio.charset.Charset

import com.typesafe.scalalogging.StrictLogging
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder
import io.rdbc.pgsql.core.SessionParams
import io.rdbc.pgsql.core.codec.Encoder
import io.rdbc.pgsql.core.messages.frontend.PgFrontendMessage

protected[netty] class PgMsgEncoderHandler(encoder: Encoder)
  extends MessageToByteEncoder[PgFrontendMessage]
    with StrictLogging {

  @volatile private var _charset = SessionParams.default.clientCharset

  def charset = _charset

  def charset_=(charset: Charset): Unit = {
    logger.debug(s"Client charset changed to '$charset'")
    _charset = charset
  }

  override def encode(ctx: ChannelHandlerContext, msg: PgFrontendMessage, out: ByteBuf): Unit = {
    out.writeBytes(encoder.encode(msg)(charset))
  }
}
