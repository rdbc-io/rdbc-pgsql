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

import io.rdbc.core.api.exceptions.ResultProcessingException
import io.rdbc.core.api.exceptions.ResultProcessingException.{MissingColumnException, NoSuitableConverterFoundException}
import io.rdbc.core.api.{Row, TypeConverterRegistry}
import io.rdbc.core.typeconv.{IntConverter, LongConverter, StringConverter}
import io.rdbc.pgsql.core.messages.backend.RowDescription
import io.rdbc.pgsql.core.messages.data.DbValFormat.{BinaryDbValFormat, TextualDbValFormat}
import io.rdbc.pgsql.core.messages.data.{DataType, FieldValue, NotNullFieldValue, NullFieldValue}
import scodec.bits.ByteVector

import scala.concurrent.Future

/**
  * Created by krzysztofpado on 21/08/16.
  */
class PgRow(rowDesc: RowDescription, cols: IndexedSeq[FieldValue], nameMapping: Map[String, Int], tconvRegistry: PgTypeConvRegistry) extends Row {

  val typeConverterRegistry = TypeConverterRegistry(StringConverter, IntConverter, LongConverter)
  //TODO

  override def obj[T](name: String, cls: Class[T]): T = {
    nameMapping.get(name) match {
      case Some(idx) => obj[T](idx, cls)
      case None => throw MissingColumnException(name)
    }
  }

  private def textualToObj(pgType: DataType, textualVal: String): Any = {
    tconvRegistry.byTypeOid(pgType.oid).map(_.toObj(textualVal))
      .getOrElse(throw new Exception("unsupported type")) //TODO
  }

  private def binaryToObj(pgType: DataType, binaryVal: ByteVector): Any = {
    tconvRegistry.byTypeOid(pgType.oid).map(_.toObj(binaryVal))
      .getOrElse(throw new Exception("unsupported type")) //TODO
  }

  override def obj[A](idx: Int, cls: Class[A]): A = {
    //TODO preconditions fucking everywhere
    //TODO for returning inserts this is 0 based, for selects not, fix this
    val fieldDesc = rowDesc.fieldDescriptions(idx) //TODO this must be indexed seq, not List
    val fieldVal = cols(idx)

    val anyFieldVal = fieldVal match {
      case NullFieldValue => null.asInstanceOf[A]
      case NotNullFieldValue(rawFieldVal) =>
        fieldDesc.fieldFormat match {
          case TextualDbValFormat => textualToObj(fieldDesc.dataType, new String(rawFieldVal.toArray)) //TODO charset
          case BinaryDbValFormat => binaryToObj(fieldDesc.dataType, rawFieldVal)
        }
    }
    if (anyFieldVal == null) null.asInstanceOf[A]
    else {
      if (cls.isInstance(anyFieldVal)) {
        anyFieldVal.asInstanceOf[A]
      } else {
        typeConverterRegistry.converters.get(cls)
          .map(converter => converter.fromAny(anyFieldVal).asInstanceOf[A])
          .getOrElse(throw NoSuitableConverterFoundException(anyFieldVal))
      }
    }
  }
}
