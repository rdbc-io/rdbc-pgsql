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

package io.rdbc.pgsql.core.types

import _root_.scodec.bits.ByteVector
import io.rdbc.pgsql.core.Oid

case object PgByteaType extends PgType[PgBytea] {
  val oid = Oid(17)
  val valCls = classOf[PgBytea]
  val name = "bytea"
}

final case class PgBytea(value: ByteVector) extends PgVal[ByteVector] {
  val typ = PgByteaType
}
