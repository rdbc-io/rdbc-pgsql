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
import io.rdbc.implbase.RowPartialImpl
import io.rdbc.pgsql.core.{ColValueToObjConverter, SessionParams}
import io.rdbc.pgsql.core.exception.PgDriverInternalErrorException
import io.rdbc.pgsql.core.internal.protocol.messages.backend.RowDescription
import io.rdbc.pgsql.core.internal.protocol.messages.frontend.ColName
import io.rdbc.pgsql.core.internal.protocol.{ColFormat, ColValue, DataType}
import io.rdbc.sapi.Row
import io.rdbc.sapi.exceptions.MissingColumnException
import io.rdbc.util.Logging
import io.rdbc.util.Preconditions._

import scala.reflect.ClassTag

private[core] class PgRow(rowDesc: RowDescription,
                          cols: IndexedSeq[ColValue],
                          nameMapping: Map[ColName, Int],
                          colValConverter: ColValueToObjConverter,
                          implicit private[this] val sessionParams: SessionParams)
  extends Row
    with RowPartialImpl
    with Logging {

  override def colOpt[A: ClassTag](idx: Int): Option[A] = {
    checkNotNull(idx)
    check(idx, idx >= 0, "has to be >= 0")
    val colVal = cols(idx)
    colVal match {
      case ColValue.Null => None
      case ColValue.NotNull(rawFieldVal) =>
        val colDesc = rowDesc.colDescs(idx)
        colDesc.format match {
          case ColFormat.Binary =>
            val targetType = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
            Some(binaryToObj(colDesc.dataType, rawFieldVal, targetType))

          case ColFormat.Textual =>
            throw new PgDriverInternalErrorException(
              s"Value '$colVal' of column '$colDesc' is in textual format, which is unsupported"
            )
        }
    }
  }

  override def colOpt[A: ClassTag](name: String): Option[A] = {
    checkNotNull(name)
    checkNonEmptyString(name)
    nameMapping.get(ColName(name)) match {
      case Some(idx) => colOpt(idx)
      case None => throw new MissingColumnException(name)
    }
  }

  private def binaryToObj[T](pgType: DataType,
                             binaryVal: ByteVector,
                             targetType: Class[T]): T = {
    throwOnFailure {
      colValConverter.colValToObj(pgType.oid, binaryVal, targetType)
    }
  }
}
