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

package io.rdbc.pgsql.core.types

import io.rdbc.pgsql.core.SessionParams
import io.rdbc.pgsql.core.pgstruct.Oid
import scodec.bits.ByteVector

trait PgType[T] {
  def typeOid: Oid
  def cls: Class[T]
  def otherClasses: Vector[Class[_]] = Vector.empty
  def name: String

  def toObj(binaryVal: ByteVector)(implicit sessionParams: SessionParams): T

  def toPgBinary(obj: T)(implicit sessionParams: SessionParams): ByteVector
}
