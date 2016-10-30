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

package io.rdbc.pgsql

import java.nio.charset.Charset

import _root_.scodec._
import _root_.scodec.bits.{BitVector, ByteVector}
import _root_.scodec.codecs._
import io.rdbc.pgsql.core.messages.backend.{FieldDescription, Header}
import io.rdbc.pgsql.core.messages.data.DbValFormat.{BinaryDbValFormat, TextualDbValFormat}
import io.rdbc.pgsql.core.messages.data.{DataType, DbValFormat, FieldValue, Oid}

package object scodec {

  def terminated[A](terminator: BitVector, codec: Codec[A]): Codec[A] = new TerminatedCodec[A](terminator, codec).withToString("terminated")

  def nulTerminated[A](codec: Codec[A]): Codec[A] = terminated(BitVector.lowByte, codec).withToString("nul terminated")

  def pgString(implicit charset: Charset) = nulTerminated(string(charset)).withToString("pgString")

  def pgStringNonTerminated(implicit charset: Charset) = string(charset).withToString("pgStringNonTerminated")

  implicit val pgInt64 = int64.withToString("pgInt64")
  implicit val pgInt32 = int32.withToString("pgInt32")
  implicit val pgInt16 = int16.withToString("pgInt16")

  implicit def pgStringOption(implicit charset: Charset) = pgString.xmap[Option[String]](s => {
    if (s.isEmpty) None
    else Some(s)
  }, {
    case Some(s) => s
    case None => ""
  })

  implicit val pgInt16Option = pgInt16.xmap[Option[Int]](i => {
    if (i == 0) None
    else Some(i)
  }, {
    case Some(i) => i
    case None => 0
  })

  implicit val pgInt32Option = pgInt32.xmap[Option[Int]](i => {
    if (i == 0) None
    else Some(i)
  }, {
    case Some(i) => i
    case None => 0
  })

  def pgParam[K](keyCodec: Codec[K])(implicit charset: Charset): Codec[(K, String)] = {
    ("key" | keyCodec) ~
      ("value" | pgString)
  }.withToString("pgParam")

  def pgParamMap[K](keyCodec: Codec[K])(implicit charset: Charset): Codec[Map[K, String]] = new PgMapCodec[K](pgParam(keyCodec))


  def fieldDescription(implicit charset: Charset): Codec[FieldDescription] = {
    {
      ("name" | pgString) ::
        ("tableOid" | oidOption) ::
        ("columnAttr" | pgInt16Option) ::
        ("dataType" | dataType) ::
        ("fieldFormatCode" | dbValFormat)
    }.as[FieldDescription].withToString("FieldDescription")
  }

  val fieldValue: Codec[FieldValue] = new FieldValueCodec

  val header: Codec[Header] = {
    {
      ("header" | ignore(8)) ~>
        ("msgLength" | pgInt32)
    }.as[Header]
  }

  val dbValFormat: Codec[DbValFormat] = {
    discriminated[DbValFormat].by(pgInt16)
      .subcaseP(0)({ case t@TextualDbValFormat => t })(provide(TextualDbValFormat))
      .subcaseP(1)({ case b@BinaryDbValFormat => b })(provide(BinaryDbValFormat))
  }

  val oid: Codec[Oid] = uint32.as[Oid].withToString("pgOid")

  val oidOption: Codec[Option[Oid]] = oid.xmap[Option[Oid]](oidVal => {
    if (oidVal.code == 0) None
    else Some(oidVal)
  }, {
    case Some(oidVal) => oidVal
    case None => Oid(0L)
  })

  val dataType: Codec[DataType] = {
    {
      ("oid" | oid) ::
        ("size" | pgInt16) ::
        ("modifier" | pgInt32)
    }.as[DataType]
  }

  def bytesArr(n: Int): Codec[Array[Byte]] = bytes(n).xmap(_.toArray, ByteVector.view)

  val bytesArr: Codec[Array[Byte]] = bytes.xmap(_.toArray, ByteVector.view)

}
