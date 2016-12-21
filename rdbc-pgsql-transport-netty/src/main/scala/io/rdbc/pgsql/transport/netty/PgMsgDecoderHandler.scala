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
import java.{util => ju}

import com.typesafe.scalalogging.StrictLogging
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import io.rdbc.pgsql.core.SessionParams
import io.rdbc.pgsql.core.codec.{Decoder, DecoderFactory}
import scodec.bits.ByteVector

private[netty] class PgMsgDecoderHandler(decoderFactory: DecoderFactory)
  extends ByteToMessageDecoder
    with StrictLogging {

  @volatile private[this] var decoder: Decoder = {
    decoderFactory.decoder(SessionParams.default.serverCharset)
  }

  def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: ju.List[AnyRef]): Unit = {
    val bytes = new Array[Byte](in.readableBytes())
    in.readBytes(bytes)
    out.add(decoder.decodeMsg(ByteVector.view(bytes)).msg)
  }

  def changeCharset(charset: Charset): Unit = {
    logger.debug(s"Message decoder charset changed to '$charset'")
    decoder = decoderFactory.decoder(charset)
  }

  //TODO should this override exceptionCaught?
}
