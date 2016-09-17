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

package io.rdbc.pgsql.transport.akkaio

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.io.Tcp.{Close => TcpClose, CommandFailed => TcpCommandFailed, Connect => TcpConnect, Connected => TcpConnected, ConnectionClosed => TcpConnectionClosed, Received => TcpReceived, Register => TcpRegister, Write => TcpWrite}
import akka.io.{IO, Tcp}
import io.rdbc.pgsql.codec.{Decoded, Decoder, Encoder}
import io.rdbc.pgsql.core.messages.backend.{Header, ServerCharset}
import io.rdbc.pgsql.core.messages.frontend.ClientCharset
import scodec.bits.ByteVector
import scodec.interop.akka._

object AkkaIoConnectionManager {
  def props(listener: ActorRef, encoder: Encoder, decoder: Decoder) = Props(classOf[AkkaIoConnectionManager], listener, encoder, decoder)
}

//TODO set supervisor strategy and generally familiarize yourself with supervision
class AkkaIoConnectionManager(listener: ActorRef, encoder: Encoder, decoder: Decoder) extends Actor with ActorLogging {

  import context._
  import io.rdbc.pgsql.transport.ConnectionManager._

  private implicit var serverCharset = ServerCharset(DefaultCharset)
  private implicit var clientCharset = ClientCharset(DefaultCharset)

  private var buf = ByteVector.empty //TODO set max buf length
  private var maybeCurrMsgLength: Option[Int] = None //TODO set max msg length

  private def initialContext: Receive = {
    case Connect(address) =>
      log.info("Connecting to {}", address)
      IO(Tcp) ! TcpConnect(remoteAddress = address)

      context become connectingContext
  }

  private def connectedContext(connection: ActorRef, remoteAddress: InetSocketAddress): Receive = {
    case Write(msgs @ _*) =>
      val msgsBytes = msgs.foldLeft(ByteVector.empty)((acc, msg) => {
        //TODO if any error occurrs in the actor, nothing is displayed anywhere, test throwing exceptions from here
        val encoded = encoder.encode(msg)
        log.debug("Writing message {}, bytes {}", msg, encoded)
        acc ++ encoded
      })
      connection ! TcpWrite(msgsBytes.toByteString)

    case TcpReceived(data) =>
      buf = buf ++ data.toByteVector

      if (maybeCurrMsgLength.isEmpty) {
        tryParsingCurrentMessageLength(connection)
      }

      while (maybeCurrMsgLength.isDefined && buf.length >= maybeCurrMsgLength.get) {
        decoder.decodeMsg(buf) match {
          case Right(Decoded(msg, remainder)) =>
            listener ! Received(msg)
            buf = remainder
            maybeCurrMsgLength = None
            tryParsingCurrentMessageLength(connection)

          case Left(err) =>
            log.error("Could not parse message body, sync with server lost: {}", err)
            onSyncError(connection, err.msg)
        }
      }

    case TcpCommandFailed(w: TcpWrite) =>
      log.warning("TCP write {} failed", w)
      listener ! WriteFailed

    case Close =>
      log.info("Closing connection on client's request")
      connection ! TcpClose

    case _: TcpConnectionClosed =>
      log.info("Connection closed")
      listener ! ConnectionClosed
      context stop self

    case newCharset: ClientCharset =>
      log.info("Changing client charset from {} to {}", clientCharset.charset, newCharset.charset)
      clientCharset = newCharset

    case newCharset: ServerCharset =>
      log.info("Changing server charset from {} to {}", serverCharset.charset, newCharset.charset)
      serverCharset = newCharset
  }

  private def connectingContext: Receive = {
    case TcpCommandFailed(c: TcpConnect) =>
      log.info("Connection to {} failed", c.remoteAddress)
      listener ! ConnectFailed
      context stop self

    case c: TcpConnected =>
      log.info("Connection to {} estabilished", c.remoteAddress)
      val connection = sender()
      connection ! TcpRegister(self)
      context become connectedContext(connection, c.remoteAddress)
      listener ! Connected
  }

  private def tryParsingCurrentMessageLength(connection: ActorRef) = {
    if (buf.length >= Header.Length) {
      decoder.decodeHeader(buf) match {
        case Right(Decoded(header, _)) =>
          maybeCurrMsgLength = Some(header.len)

        case Left(err) =>
          log.error("Could not parse message header, sync with server lost: {}", err.msg)
          onSyncError(connection, err.msg)
      }
    }
  }

  private def onSyncError(connection: ActorRef, errMsg: String): Unit = {
    listener ! SyncError(s"Sync with server lost: $errMsg")
    connection ! TcpClose
    context stop self
  }

  override val receive = initialContext
}
