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

package io.rdbc.pgsql.core

import io.rdbc._
import io.rdbc.pgsql.core.internal.protocol.messages.frontend.PgFrontendMessage

import scala.concurrent.Future
import scala.util.Try

trait ChannelWriter {
  def write(msgs: PgFrontendMessage*): Future[Unit]

  def close(): Future[Unit]

  def flush(): Try[Unit]

  def writeAndFlush(msgs: ImmutSeq[PgFrontendMessage]): Future[Unit] = {
    writeAndFlush(msgs: _*)
  }

  def writeAndFlush(msgs: PgFrontendMessage*): Future[Unit] = {
    val fut = write(msgs: _*)
    flush()
    fut
  }

  def write(msgs: ImmutSeq[PgFrontendMessage]): Future[Unit] = write(msgs: _*)
}
