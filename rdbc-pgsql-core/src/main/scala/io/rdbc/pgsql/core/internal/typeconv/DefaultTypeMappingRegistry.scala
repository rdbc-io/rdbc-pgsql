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

import io.rdbc.pgsql.core.exception.{PgInvalidConfigException, PgUnsupportedClassException}
import io.rdbc.pgsql.core.typeconv.{TypeMapping, TypeMappingRegistry}
import io.rdbc.pgsql.core.types.{PgType, PgVal}
import io.rdbc.util.Preconditions.checkNotNull

import scala.util.{Failure, Success, Try}

private[core] class DefaultTypeMappingRegistry(mappings: Vector[TypeMapping[_, _ <: PgType[_ <: PgVal[_]]]])
  extends TypeMappingRegistry {

  val mapping: Map[Class[_], PgType[_ <: PgVal[_]]] = {
    mappings.groupBy(_.cls).map { case (cls, singleClassMappings) =>
      val pgType = if (singleClassMappings.size > 1) {
        throw new PgInvalidConfigException(
          s"Multiple type mappings registered for the same class: $singleClassMappings"
        )
      } else {
        singleClassMappings.head.pgType
      }

      cls -> pgType
    }.toMap
  }

  def classToPgType(cls: Class[_]): Try[PgType[_ <: PgVal[_]]] = {
    checkNotNull(cls)
    mapping.get(cls)
      .map(Success(_))
      .getOrElse(Failure(new PgUnsupportedClassException(
        s"Could not find PG type mapping for class ${cls.getCanonicalName}"))
      )
  }
}
