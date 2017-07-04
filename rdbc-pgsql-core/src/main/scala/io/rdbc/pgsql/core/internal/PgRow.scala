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

import _root_.scodec.bits.ByteVector
import io.rdbc.api.exceptions.MissingColumnException
import io.rdbc.implbase.RowPartialImpl
import io.rdbc.pgsql.core.SessionParams
import io.rdbc.pgsql.core.exception.{PgDriverInternalErrorException, PgUnsupportedType}
import io.rdbc.pgsql.core.pgstruct.messages.backend.RowDescription
import io.rdbc.pgsql.core.pgstruct.messages.frontend.ColName
import io.rdbc.pgsql.core.pgstruct.{ColFormat, ColValue, DataType}
import io.rdbc.pgsql.core.types.PgTypeRegistry
import io.rdbc.sapi.{Row, TypeConverterRegistry}
import io.rdbc.util.Logging
import io.rdbc.util.Preconditions._

import scala.util.Failure

private[core] class PgRow(rowDesc: RowDescription,
                          cols: IndexedSeq[ColValue],
                          nameMapping: Map[ColName, Int],
                          protected val typeConverters: TypeConverterRegistry,
                          pgTypes: PgTypeRegistry,
                          implicit private[this] val sessionParams: SessionParams)
  extends Row
    with RowPartialImpl
    with Logging {

  protected def any(name: String): Option[Any] = traced {
    argsNotNull()
    checkNonEmptyString(name)
    nameMapping.get(ColName(name)) match {
      case Some(idx) => any(idx)
      case None => throw new MissingColumnException(name)
    }
  }

  protected def any(idx: Int): Option[Any] = traced {
    argsNotNull()
    check(idx, idx >= 0, "has to be >= 0")
    val colVal = cols(idx)
    colVal match {
      case ColValue.Null => None
      case ColValue.NotNull(rawFieldVal) =>
        val colDesc = rowDesc.colDescs(idx)
        colDesc.format match {
          case ColFormat.Binary => Some(binaryToObj(colDesc.dataType, rawFieldVal))
          case ColFormat.Textual =>
              throw new PgDriverInternalErrorException(
                s"Value '$colVal' of column '$colDesc' is in textual format, which is unsupported"
              )
        }
    }
  }

  private def binaryToObj(pgType: DataType, binaryVal: ByteVector): Any = traced {
    throwOnFailure {
      pgTypes.typeByOid(pgType.oid) match {
        case None => Failure(new PgUnsupportedType(pgType))
        case Some(t) => t.toObj(binaryVal)
      }
    }
  }
}
