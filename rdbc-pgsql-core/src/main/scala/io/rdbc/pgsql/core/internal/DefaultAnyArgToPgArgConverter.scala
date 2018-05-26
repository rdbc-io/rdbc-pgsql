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

import java.util.Optional

import io.rdbc.japi
import io.rdbc.pgsql.core.internal.protocol.Argument
import io.rdbc.pgsql.core.typeconv.{TypeConverter, TypeMappingRegistry}
import io.rdbc.pgsql.core.types.{AnyPgValCodec, PgUnknownType, PgVal}
import io.rdbc.pgsql.core.{AnyArgToPgArgConverter, SessionParams}
import io.rdbc.sapi.SqlNull

import scala.util.{Success, Try}

private[core] class DefaultAnyArgToPgArgConverter(converter: TypeConverter,
                                                  mapping: TypeMappingRegistry,
                                                  codec: AnyPgValCodec)
  extends AnyArgToPgArgConverter {

  private val UnknownNullPgArg = Success(Argument.Null(PgUnknownType.oid))

  def anyToPgArg(arg: Any)(implicit sessionParams: SessionParams): Try[Argument] = {
    arg match {
      case null | None => UnknownNullPgArg
      case o: Optional[_] if !o.isPresent => UnknownNullPgArg
      case SqlNull(cls) => typedNullToPgArg(cls)
      case nul: japi.SqlNull[_] => typedNullToPgArg(nul.getType)
      case Some(value) => nonEmptyArgToPgArg(value)
      case o: Optional[_] if o.isPresent => nonEmptyArgToPgArg(o.get())
      case _ => nonEmptyArgToPgArg(arg)
    }
  }

  private def typedNullToPgArg(cls: Class[_]): Try[Argument] = {
    mapping.classToPgType(cls).map(pgType => Argument.Null(pgType.oid))
  }

  private def nonEmptyArgToPgArg(arg: Any)(implicit sessionParams: SessionParams): Try[Argument] = {
    val pgTypeTry = arg match {
      case pgVal: PgVal[_] => Success(pgVal.typ)
      case _ => mapping.classToPgType(arg.getClass)
    }

    pgTypeTry.flatMap { pgType =>
      converter.convert(arg, pgType.valCls).flatMap { pgVal =>
        codec.toBinary(pgVal).map { binary =>
          Argument.Binary(binary, pgType.oid)
        }
      }
    }
  }
}
