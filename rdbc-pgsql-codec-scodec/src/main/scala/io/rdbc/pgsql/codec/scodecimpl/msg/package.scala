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

package io.rdbc.pgsql.codec.scodecimpl

import io.rdbc.pgsql.core.messages.PgMessage
import scodec.Codec
import scodec.codecs._
import io.rdbc.pgsql.codec.scodecimpl.pg._

package object msg {

  def pgHeadlessMsg[A <: PgMessage](bodyCodec: Codec[A]): Codec[A] = {
    variableSizeBytes(pgInt32, bodyCodec, 4)
  }

  def pgHeadedMsg[A <: PgMessage](head: Byte)(bodyCodec: Codec[A]): Codec[A] = {
    byte.unit(head) ~> pgHeadlessMsg(bodyCodec)
  }

  def pgSingletonHeadedMsg[A <: PgMessage](head: Byte, singleton: A): Codec[A] = {
    byte.unit(head) ~> pgHeadlessMsg(provide(singleton))
  }

  def pgSingletonMsg[A <: PgMessage](singleton: A): Codec[A] = {
    pgInt32.withContext("length").unit(4).xmap(_ => singleton, _ => Unit)
  }
}
