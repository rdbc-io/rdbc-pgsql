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

package io.rdbc.pgsql.core.types

import _root_.scodec.bits.ByteVector
import io.rdbc.pgsql.core.internal.typecodec.CompositeAnyPgValCodec
import io.rdbc.pgsql.core.{Oid, SessionParams}
import io.rdbc.util.Preconditions.checkNotNull

import scala.util.Try

trait AnyPgValCodec {

  def toObj(oid: Oid, binaryVal: ByteVector)(implicit sessionParams: SessionParams): Try[PgVal[_]]

  def toBinary(pgVal: PgVal[_])(implicit sessionParams: SessionParams): Try[ByteVector]
}

object AnyPgValCodec {
  def fromCodecs(codecs: Vector[PgValCodec[_ <: PgVal[_]]]): AnyPgValCodec = {
    checkNotNull(codecs)
    new CompositeAnyPgValCodec(codecs)
  }
}
