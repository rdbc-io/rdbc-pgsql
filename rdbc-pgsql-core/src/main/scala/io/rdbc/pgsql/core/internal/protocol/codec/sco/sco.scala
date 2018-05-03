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

package io.rdbc.pgsql.core.internal.protocol.codec

import java.nio.charset.Charset

import io.rdbc.pgsql.core.Oid
import io.rdbc.pgsql.core.internal.protocol._
import io.rdbc.pgsql.core.internal.protocol.messages.PgMessage
import io.rdbc.pgsql.core.internal.protocol.messages.backend.MsgHeader
import io.rdbc.pgsql.core.internal.protocol.messages.frontend.ColName
import scodec.Codec
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs._

package object sco {

  private[sco] def maybe[A](codec: Codec[A], noneVal: A): Codec[Option[A]] = {
    codec.xmap[Option[A]](v => {
      if (v == noneVal) None
      else Some(v)
    }, {
      case Some(v) => v
      case None => noneVal
    }).withToString("maybe " + codec.toString)
  }

  private[sco] val maybeInt16: Codec[Option[Int]] = maybe(int16, 0)

  private[sco] val maybeInt32: Codec[Option[Int]] = maybe(int32, 0)

  private[sco] val colValFormat: Codec[ColFormat] = {
    discriminated[ColFormat]
      .by(int16)
      .subcaseP(0)({ case t@ColFormat.Textual => t })(provide(ColFormat.Textual))
      .subcaseP(1)({ case b@ColFormat.Binary => b })(provide(ColFormat.Binary))
  }

  private[sco] val oid: Codec[Oid] = uint32.as[Oid].withToString("pgOid")

  private[sco] val maybeOid: Codec[Option[Oid]] = maybe(oid, Oid(0L))

  private[sco] val dataType: Codec[DataType] = {
    {
      ("oid"      | oid) ::
      ("size"     | int16).as[DataType.Size] ::
      ("modifier" | int32).as[DataType.Modifier]
    }.as[DataType]
  }

  private[sco] val bytesArr: Codec[Array[Byte]] = bytes.xmap(_.toArray, ByteVector.view)

  private[core] val header: Codec[MsgHeader] = {
    {
      ("header"    | ignore(8)) ~>
      ("msgLength" | int32)
    }.as[MsgHeader]
  }

  private[sco] def pgHeadlessMsg[A <: PgMessage](bodyCodec: Codec[A]): Codec[A] = {
    variableSizeBytes(int32, bodyCodec, 4)
  }

  private[sco] def pgHeadedMsg[A <: PgMessage](head: Byte)(bodyCodec: Codec[A]): Codec[A] = {
    byte.unit(head) ~> pgHeadlessMsg(bodyCodec)
  }

  private[sco] def pgSingletonHeadedMsg[A <: PgMessage](head: Byte, singleton: A): Codec[A] = {
    byte.unit(head) ~> pgHeadlessMsg(provide(singleton))
  }

  private[sco] def pgSingletonHeadlessMsg[A <: PgMessage](singleton: A): Codec[A] = {
    int32.withContext("header_length").unit(4).xmap[A](_ => singleton, _ => ())
  }

  private[sco] def pgParamMap[K](keyCodec: Codec[K])(implicit charset: Charset): Codec[Map[K, String]] = {
    new PgMapCodec[K](pgParam(keyCodec)).withContext("pg_params")
  }

  private[sco] def terminated[A](terminator: BitVector, codec: Codec[A]): Codec[A] = {
    new TerminatedCodec[A](terminator, codec).withToString("terminated")
  }

  private[sco] def stringNul(implicit charset: Charset): Codec[String] = {
    terminated(BitVector.lowByte, string(charset)).withToString("stringNul")
  }

  private[sco] def maybeStringNul(implicit charset: Charset): Codec[Option[String]] = {
    maybe(stringNul(charset), "")
  }

  private[sco] def colName(implicit charset: Charset): Codec[ColName] = stringNul.as[ColName]

  private[sco] def pgParam[K](keyCodec: Codec[K])(implicit charset: Charset): Codec[(K, String)] = {
    ("key" | keyCodec) ~ ("value" | stringNul)
  }.withToString("pgParam")

  private[sco] def colDesc(implicit charset: Charset): Codec[ColDesc] = {
    {
      ("name"          | colName) ::
      ("tableOid"      | maybeOid) ::
      ("columnAttr"    | maybeInt16) ::
      ("dataType"      | dataType) ::
      ("colFormatCode" | colValFormat)
    }.as[ColDesc].withToString("ColDesc")
  }

  private[sco] val colValue: Codec[ColValue] = ColValueCodec

}
