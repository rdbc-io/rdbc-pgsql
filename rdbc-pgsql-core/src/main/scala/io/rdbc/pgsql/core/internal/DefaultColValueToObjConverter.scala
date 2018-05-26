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
import io.rdbc.pgsql.core.typeconv.TypeConverter
import io.rdbc.pgsql.core.types.AnyPgValCodec
import io.rdbc.pgsql.core.{ColValueToObjConverter, Oid, SessionParams}

import scala.util.{Success, Try}

private[core] class DefaultColValueToObjConverter(codec: AnyPgValCodec,
                                                  converter: TypeConverter)
  extends ColValueToObjConverter {

  def colValToObj[T](oid: Oid,
                     binaryVal: ByteVector,
                     targetCls: Class[T])(implicit sessionParams: SessionParams): Try[T] = {
    codec.toObj(oid, binaryVal).flatMap { pgVal =>
      if (targetCls.isInstance(pgVal)) {
        Success(pgVal.asInstanceOf[T])
      } else if (targetCls.isInstance(pgVal.value)) {
        //TODO this condition is false for primitives and a converter needs to kick in
        //consider optimizing
        Success(pgVal.value.asInstanceOf[T])
      } else {
        converter.convert(pgVal, targetCls)
      }
    }
  }
}
