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

import io.rdbc.pgsql.core.messages.data.Oid
import scodec.bits.ByteVector

class Int4TypeConv extends PgTypeConv {
  val typeOid = Oid(23)
  val classes = Seq(classOf[Integer], classOf[Int])
  val primaryClass = classes.head

  override def toObj(textualVal: String): Int = textualVal.toInt

  override def toObj(binaryVal: ByteVector): Int = ???

  override def toPgTextual(obj: Any): String = obj.asInstanceOf[Int].toString

  override def toPgBinary(obj: Any): ByteVector = ???

}
