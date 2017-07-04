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

package io.rdbc.pgsql.core.internal

import _root_.scodec._
import _root_.scodec.bits.ByteVector
import _root_.scodec.codecs._
import io.rdbc.pgsql.core.pgstruct._

package object scodec {

  private[scodec] def maybe[A](codec: Codec[A], noneVal: A): Codec[Option[A]] = {
    codec.xmap[Option[A]](v => {
      if (v == noneVal) None
      else Some(v)
    }, {
      case Some(v) => v
      case None => noneVal
    }).withToString("maybe " + codec.toString)
  }

  private[scodec] val maybeInt16: Codec[Option[Int]] = maybe(int16, 0)

  private[scodec] val maybeInt32: Codec[Option[Int]] = maybe(int32, 0)

  private[scodec] val colValFormat: Codec[ColFormat] = {
    discriminated[ColFormat]
      .by(int16)
      .subcaseP(0)({ case t@ColFormat.Textual => t })(provide(ColFormat.Textual))
      .subcaseP(1)({ case b@ColFormat.Binary => b })(provide(ColFormat.Binary))
  }

  private[scodec] val oid: Codec[Oid] = uint32.as[Oid].withToString("pgOid")

  private[scodec] val maybeOid: Codec[Option[Oid]] = maybe(oid, Oid(0L))

  private[scodec] val dataType: Codec[DataType] = {
    {
      ("oid" | oid) ::
        ("size" | int16).as[DataType.Size] ::
        ("modifier" | int32).as[DataType.Modifier]
    }.as[DataType]
  }

  private[scodec] val bytesArr: Codec[Array[Byte]] = bytes.xmap(_.toArray, ByteVector.view)
}
