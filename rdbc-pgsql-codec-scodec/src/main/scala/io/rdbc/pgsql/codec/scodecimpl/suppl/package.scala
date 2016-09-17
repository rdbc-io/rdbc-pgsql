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

import java.nio.charset.Charset

import io.rdbc.pgsql.core.messages.backend.{FieldDescription, Header}
import io.rdbc.pgsql.core.messages.data.DbValFormat.{BinaryDbValFormat, TextualDbValFormat}
import io.rdbc.pgsql.core.messages.data.{DataType, DbValFormat, FieldValue, Oid}
import scodec.Codec
import scodec.bits.BitVector
import pg._
import scodec.codecs._

package object suppl {

  def terminated[A](terminator: BitVector, codec: Codec[A]): Codec[A] = new TerminatedCodec[A](terminator, codec).withToString("terminated")

  def nulTerminated[A](codec: Codec[A]): Codec[A] = terminated(BitVector.lowByte, codec).withToString("nul terminated")

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
    if(oidVal.code == 0) None
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

}
