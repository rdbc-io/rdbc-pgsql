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

package io.rdbc.pgsql.core.internal.typecodec

import io.rdbc.pgsql.core.exception.{PgInvalidConfigException, PgUnsupportedTypeException}
import io.rdbc.pgsql.core.types.{AnyPgValCodec, PgVal, PgValCodec}
import io.rdbc.pgsql.core.{Oid, SessionParams}
import io.rdbc.util.Preconditions.checkNotNull
import scodec.bits.ByteVector

import scala.util.{Failure, Success, Try}

private[core] class CompositeAnyPgValCodec(codecs: Vector[PgValCodec[_ <: PgVal[_]]])
  extends AnyPgValCodec {

  val codecMap: Map[Oid, PgValCodec[_ <: PgVal[_]]] = {
    codecs.groupBy(_.typ).map { case (pgType, sameTypeCodecs) =>
      val codec = if (sameTypeCodecs.length > 1) {
        throw new PgInvalidConfigException(
          s"Multiple codecs registered for type $pgType"
        )
      } else {
        sameTypeCodecs.head
      }

      pgType.oid -> codec
    }.toMap
  }

  private def codecForOid(oid: Oid): Try[PgValCodec[_ <: PgVal[_]]] = {
    codecMap.get(oid)
      .map(Success(_))
      .getOrElse(Failure(new PgUnsupportedTypeException(oid)))
  }

  def toObj(oid: Oid, binaryVal: ByteVector)(implicit sessionParams: SessionParams): Try[PgVal[_]] = {
    checkNotNull(oid)
    checkNotNull(binaryVal)
    codecForOid(oid).flatMap { codec =>
      codec.toObj(binaryVal)
    }
  }

  def toBinary(pgVal: PgVal[_])(implicit sessionParams: SessionParams): Try[ByteVector] = {
    checkNotNull(pgVal)
    codecForOid(pgVal.typ.oid).flatMap { codec =>
      codec.asInstanceOf[PgValCodec[pgVal.type]].toBinary(pgVal)
    }
  }
}
