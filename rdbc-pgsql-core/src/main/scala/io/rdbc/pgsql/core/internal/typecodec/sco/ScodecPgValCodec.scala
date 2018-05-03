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

package io.rdbc.pgsql.core.internal.typecodec.sco

import io.rdbc.pgsql.core.SessionParams
import io.rdbc.pgsql.core.exception.{PgDecodeException, PgEncodeException}
import io.rdbc.pgsql.core.types.{PgType, PgVal, PgValCodec}
import scodec.Attempt.{Failure, Successful}
import scodec.Codec
import scodec.bits.ByteVector

import scala.util.{Success, Try}

private[typecodec] abstract class ScodecPgValCodec[T <: PgVal[_]]
  extends PgValCodec[T] {

  def typ: PgType[T]

  def codec(sessionParams: SessionParams): Codec[T]

  override def toObj(binaryVal: ByteVector)(implicit sessionParams: SessionParams): Try[T] = {
    codec(sessionParams).decodeValue(binaryVal.bits) match {
      case Successful(value) => Success(value)
      case Failure(err) => util.Failure(new PgDecodeException(
        s"Error decoding binary '${binaryVal.toHex}' of PG type '${typ.name}' as '${typ.valCls}': "
          + err.messageWithContext
      ))
    }
  }

  override def toBinary(pgVal: T)(implicit sessionParams: SessionParams): Try[ByteVector] = {
    codec(sessionParams).encode(pgVal) match {
      case Successful(value) => Success(value.bytes)
      case Failure(err) => util.Failure(new PgEncodeException(
        s"Error encoding '$pgVal' of PG type '${typ.name}' to binary: "
          + err.messageWithContext
      ))
    }
  }

}
