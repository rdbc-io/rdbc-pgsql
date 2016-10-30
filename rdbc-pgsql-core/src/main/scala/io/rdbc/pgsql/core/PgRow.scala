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

import io.rdbc.api.exceptions.ResultProcessingException.MissingColumnException
import io.rdbc.implbase.RowPartialImpl
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
      case None => throw MissingColumnException(name)
    }
  }

  protected def notConverted(idx: Int): Any = {
    //TODO preconditions fucking everywhere
    //TODO for returning inserts this is 0 based, for selects not, fix this
    val fieldDesc = rowDesc.fieldDescriptions(idx)
    //TODO this must be indexed seq, not List
    val fieldVal = cols(idx)

    fieldVal match {
      case NullFieldValue => null
      case NotNullFieldValue(rawFieldVal) =>
        fieldDesc.fieldFormat match {
          case TextualDbValFormat => textualToObj(fieldDesc.dataType, new String(rawFieldVal)) //TODO charset
          case BinaryDbValFormat => binaryToObj(fieldDesc.dataType, rawFieldVal)
        }
    }
  }

  private def textualToObj(pgType: DataType, textualVal: String): Any = {
    pgTypeConvRegistry.byTypeOid(pgType.oid).map(_.toObj(textualVal))
      .getOrElse(throw new Exception(s"unsupported type $pgType")) //TODO
  }

  private def binaryToObj(pgType: DataType, binaryVal: Array[Byte]): Any = {
    pgTypeConvRegistry.byTypeOid(pgType.oid).map(_.toObj(binaryVal))
      .getOrElse(throw new Exception(s"unsupported type $pgType")) //TODO
  }
}
