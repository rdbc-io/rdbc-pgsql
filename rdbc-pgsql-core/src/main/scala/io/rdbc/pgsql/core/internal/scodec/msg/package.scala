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

package io.rdbc.pgsql.core.internal.scodec

import java.nio.charset.Charset

import _root_.scodec.Codec
import _root_.scodec.codecs._
import io.rdbc.pgsql.core.pgstruct.messages.PgMessage
import io.rdbc.pgsql.core.pgstruct.messages.backend.MsgHeader
import io.rdbc.pgsql.core.pgstruct.messages.frontend.ColName
import io.rdbc.pgsql.core.pgstruct.{ColDesc, ColValue}
import scodec.bits.BitVector

package object msg {

  private[core] val header: Codec[MsgHeader] = {
    {
      ("header" | ignore(8)) ~>
        ("msgLength" | int32)
    }.as[MsgHeader]
  }

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
    int32.withContext("header_length").unit(4).xmap[A](_ => singleton, _ => ())
  }

  private[msg] def pgParamMap[K](keyCodec: Codec[K])(implicit charset: Charset): Codec[Map[K, String]] = {
    new PgMapCodec[K](pgParam(keyCodec)).withContext("pg_params")
  }

  private[msg] def terminated[A](terminator: BitVector, codec: Codec[A]): Codec[A] = {
    new TerminatedCodec[A](terminator, codec).withToString("terminated")
  }

  private[msg] def stringNul(implicit charset: Charset): Codec[String] = {
    terminated(BitVector.lowByte, string(charset)).withToString("stringNul")
  }

  private[msg] def maybeStringNul(implicit charset: Charset): Codec[Option[String]] = {
    maybe(stringNul(charset), "")
  }

  private[msg] def colName(implicit charset: Charset): Codec[ColName] = stringNul.as[ColName]

  private[msg] def pgParam[K](keyCodec: Codec[K])(implicit charset: Charset): Codec[(K, String)] = {
    ("key" | keyCodec) ~ ("value" | stringNul)
  }.withToString("pgParam")

  private[msg] def colDesc(implicit charset: Charset): Codec[ColDesc] = {
    {
      ("name" | colName) ::
        ("tableOid" | maybeOid) ::
        ("columnAttr" | maybeInt16) ::
        ("dataType" | dataType) ::
        ("colFormatCode" | colValFormat)
    }.as[ColDesc].withToString("ColDesc")
  }

  private[msg] val colValue: Codec[ColValue] = ColValueCodec

}
