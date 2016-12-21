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

package io.rdbc.pgsql.scodec

import _root_.scodec.Codec
import _root_.scodec.codecs._
import io.rdbc.pgsql.core.pgstruct.messages.PgMessage

package object msg {

  private[msg] def pgHeadlessMsg[A <: PgMessage](bodyCodec: Codec[A]): Codec[A] = {
    variableSizeBytes(int32, bodyCodec, 4)
  }

  private[msg] def pgHeadedMsg[A <: PgMessage](head: Byte)(bodyCodec: Codec[A]): Codec[A] = {
    byte.unit(head) ~> pgHeadlessMsg(bodyCodec)
  }

  private[msg] def pgSingletonHeadedMsg[A <: PgMessage](head: Byte, singleton: A): Codec[A] = {
    byte.unit(head) ~> pgHeadlessMsg(provide(singleton))
  }

  private[msg] def pgSingletonHeadlessMsg[A <: PgMessage](singleton: A): Codec[A] = {
    int32.withContext("header_length").unit(4).xmap[A](_ => singleton, _ => Unit)
  }
}
