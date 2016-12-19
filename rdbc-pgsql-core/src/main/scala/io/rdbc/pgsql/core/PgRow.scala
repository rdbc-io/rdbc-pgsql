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

package io.rdbc.pgsql.core

import io.rdbc.api.exceptions.MissingColumnException
import io.rdbc.implbase.RowPartialImpl
import io.rdbc.pgsql.core.exception.{PgDriverInternalErrorException, PgUnsupportedType}
import io.rdbc.pgsql.core.messages.backend.RowDescription
import io.rdbc.pgsql.core.messages.data.DbValFormat.{BinaryDbValFormat, TextualDbValFormat}
import io.rdbc.pgsql.core.messages.data.{DataType, FieldValue, NotNullFieldValue, NullFieldValue}
import io.rdbc.pgsql.core.types.PgTypeRegistry
import io.rdbc.sapi.{Row, TypeConverterRegistry}

class PgRow(rowDesc: RowDescription,
            cols: IndexedSeq[FieldValue],
            nameMapping: Map[String, Int],
            rdbcTypeConvRegistry: TypeConverterRegistry,
            pgTypeConvRegistry: PgTypeRegistry,
            implicit val sessionParams: SessionParams)
  extends Row with RowPartialImpl {

  val typeConverterRegistry = rdbcTypeConvRegistry

  protected def notConverted(name: String): Any = {
    nameMapping.get(name) match {
      case Some(idx) => notConverted(idx)
      case None => throw new MissingColumnException(name)
    }
  }

  protected def notConverted(idx: Int): Any = {
    val fieldVal = cols(idx)
    fieldVal match {
      case NullFieldValue => null
      case NotNullFieldValue(rawFieldVal) =>
        val fieldDesc = rowDesc.fieldDescriptions(idx)
        fieldDesc.fieldFormat match {
          case BinaryDbValFormat => binaryToObj(fieldDesc.dataType, rawFieldVal)
          case TextualDbValFormat => throw PgDriverInternalErrorException(s"Value '$fieldVal' of field '$fieldDesc' is in textual format, which is unsupported")
        }
    }
  }

  private def binaryToObj(pgType: DataType, binaryVal: Array[Byte]): Any = {
    pgTypeConvRegistry.byTypeOid(pgType.oid).map(_.toObj(binaryVal))
      .getOrElse(throw new PgUnsupportedType(pgType))
  }
}
