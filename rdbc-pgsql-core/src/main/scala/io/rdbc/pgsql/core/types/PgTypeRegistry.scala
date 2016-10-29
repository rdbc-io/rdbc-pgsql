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

import io.rdbc.pgsql.core.messages.data.Oid

object PgTypeRegistry {

  def apply(types: PgType[_]*): PgTypeRegistry = {
    val oid2type = Map(
      types.map(t => t.typeOid -> t): _*
    )

    val cls2type: Map[Class[_], PgType[_]] = Map(
      types.flatMap { t =>
        t.otherClasses.map(oc => oc -> t) :+ (t.cls -> t)
      }: _*
    )

    new PgTypeRegistry(oid2type, cls2type)
  }
}

class PgTypeRegistry(val oid2type: Map[Oid, PgType[_]], val cls2type: Map[Class[_], PgType[_]]) {

  def byClass[T](cls: Class[T]): Option[PgType[T]] = {
    cls2type.get(cls).map(_.asInstanceOf[PgType[T]])
  }

  def byTypeOid(oid: Oid): Option[PgType[_]] = {
    oid2type.get(oid)
  }

}