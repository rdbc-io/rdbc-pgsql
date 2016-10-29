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
import java.util

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import io.rdbc.pgsql.core.codec.{Decoded, Decoder, DecodingError}
import io.rdbc.pgsql.core.messages.backend.ServerCharset
import scodec.bits.ByteVector

protected[netty] class NettyPgMsgDecoder(decoder: Decoder) extends ByteToMessageDecoder {

  @volatile
  implicit private var _charset = ServerCharset(Charset.forName("US-ASCII"))

  def charset = _charset.charset

  def charset_=(charset: Charset): Unit = {
    println(s"server charset changed to $charset")
    _charset = ServerCharset(charset)
  }

  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    val bytes = new Array[Byte](in.readableBytes())
    in.readBytes(bytes)
    decoder.decodeMsg(ByteVector.view(bytes)) match {
      case Left(DecodingError(err)) => throw new Exception(err) //TODO exception
      case Right(Decoded(backendMsg, _)) => out.add(backendMsg)
    }
  }
}
