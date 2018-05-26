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

package io.rdbc.pgsql.core.internal.typeconv

import io.rdbc.japi
import io.rdbc.pgsql.core.internal.typeconv.extractors._
import io.rdbc.pgsql.core.typeconv.PartialTypeConverter
import io.rdbc.sapi.{SqlBinary, SqlBlob}
import scodec.bits.ByteVector

private[typeconv] object ByteVectorTypeConverter
  extends PartialTypeConverter[ByteVector] {

  val cls = classOf[ByteVector]

  def convert(any: Any): Option[ByteVector] = {
    any match {
      case ByteVectorVal(bytes) => Some(bytes)
      case ByteArrayVal(byteArr) => Some(ByteVector(byteArr))
      case SqlBlob(byteSeq) => Some(ByteVector(byteSeq))
      case sb: japi.SqlBlob => Some(ByteVector(sb.getValue))
      case SqlBinary(byteSeq) => Some(ByteVector(byteSeq))
      case _ => None
    }
  }
}
