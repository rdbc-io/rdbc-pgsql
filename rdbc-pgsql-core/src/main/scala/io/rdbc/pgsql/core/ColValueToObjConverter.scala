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

package io.rdbc.pgsql.core

import io.rdbc.pgsql.core.internal.DefaultColValueToObjConverter
import io.rdbc.pgsql.core.typeconv.TypeConverter
import io.rdbc.pgsql.core.types.AnyPgValCodec
import scodec.bits.ByteVector

import scala.util.Try

trait ColValueToObjConverter {
  def colValToObj[T](oid: Oid,
                     binaryVal: ByteVector,
                     targetCls: Class[T])(implicit sessionParams: SessionParams): Try[T]
}

object ColValueToObjConverter {
  def fromCodecAndTypeconv(codec: AnyPgValCodec, converter: TypeConverter): ColValueToObjConverter = {
    new DefaultColValueToObjConverter(codec, converter)
  }
}
