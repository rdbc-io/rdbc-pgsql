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

import io.netty.channel.Channel
import io.rdbc.pgsql.core.ChannelWriter
import io.rdbc.pgsql.core.exception.PgChannelException
import io.rdbc.pgsql.core.pgstruct.messages.frontend.PgFrontendMessage
import io.rdbc.util.Logging

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

private[netty] class NettyChannelWriter(ch: Channel)
                                       (implicit ec: ExecutionContext)
  extends ChannelWriter
    with Logging {

  def write(msgs: PgFrontendMessage*): Future[Unit] = {
    msgs
      .foldLeft(Future.successful(())) { (_, msg) =>
        logger.trace(s"Writing message '$msg' to channel $ch")
        ch.write(msg).scalaFut.map(_ => ())
      }
      .recoverWith {
        case NonFatal(ex) => Future.failed(new PgChannelException(ex))
      }
  }

  def close(): Future[Unit] = {
    logger.debug(s"Closing channel $ch")
    ch.close().scalaFut.map(_ => ()).recoverWith {
      case NonFatal(ex) => Future.failed(new PgChannelException(ex))
    }
  }

  def flush(): Unit = {
    logger.trace(s"Flushing channel $ch")
    try {
      ch.flush()
      ()
    } catch {
      case NonFatal(ex) => throw new PgChannelException(ex)
    }
  }
}
