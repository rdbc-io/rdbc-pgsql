/*
 * Copyright 2016-2017 Krzysztof Pado
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

package io.rdbc.pgsql

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.rdbc.pgsql.core.internal.fsm.StateAction
import io.rdbc.pgsql.core.pgstruct.{Argument, TxStatus}
import io.rdbc.pgsql.core.pgstruct.messages.backend.{BackendKeyData, PgBackendMessage}

import scala.concurrent.Future

package object core {

  type RequestCanceler = BackendKeyData => Future[Unit]

  case class ConnId(value: String) extends AnyVal

  private[core] type FatalErrorNotifier = (String, Throwable) => Unit

  private[core] case class RequestId(connId: ConnId, value: Long) {
    override def toString: String = s"${connId.value}:$value"
  }

  private[core] case class RdbcSql(value: String) extends AnyVal

  private[core] case class StmtParamName(value: String) extends AnyVal

  private[core] type ClientRequest[A] = (RequestId, TxStatus) => Future[A]

  private[core] type PgMsgHandler = PartialFunction[PgBackendMessage, StateAction]

  private[core] type ArgsSource = Source[Vector[Argument], NotUsed]

}
