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

import io.rdbc.ImmutIndexedSeq
import io.rdbc.sapi.exceptions.{MissingParamValException, NoSuchParamException, NoSuitableConverterFoundException}
import io.rdbc.pgsql.core.SessionParams
import io.rdbc.pgsql.core.internal.PgNativeStatement.Params
import io.rdbc.pgsql.core.internal.PgNativeStatement.Params.Named
import io.rdbc.pgsql.core.pgstruct.{Argument, Oid}
import io.rdbc.pgsql.core.types.{PgType, PgTypeRegistry}
import io.rdbc.sapi.{NotNullParam, NullParam}
import io.rdbc.util.Logging

import scala.util.{Failure, Success, Try}

class StmtArgConverter(pgTypes: PgTypeRegistry, sessionParams: SessionParams)
  extends Logging {

  def convertPositionalArgs(posArgs: ImmutIndexedSeq[Any]): Try[Vector[Argument]] = {
    posArgs.foldLeft(Try(Vector.empty[Argument])) { (accTry, anyArg) =>
      accTry.flatMap { acc =>
        anyToPgArgument(anyArg).map { arg =>
          acc :+ arg
        }
      }
    }
  }

  def convertNamedArgs(namedArgs: Map[String, Any], namedParams: Params.Named): Try[Vector[Argument]] = traced {
    case class Acc(res: Vector[Argument], remaining: Set[String])

    val Named(params) = namedParams
    val pgParamsMap = namedArgs.mapValues(anyToPgArgument)
    val init = Try(Acc(res = Vector.empty, remaining = namedArgs.keySet))
    val indexedPgParamsTry = params.foldLeft(init) { (accTry, paramName) =>
      accTry.flatMap { acc =>
        pgParamsMap.getOrElse(paramName, Failure(new MissingParamValException(paramName)))
          .map { arg =>
            acc.copy(
              res = acc.res :+ arg,
              remaining = acc.remaining - paramName
            )
          }
      }
    }
    indexedPgParamsTry.flatMap { indexedPgParams =>
      indexedPgParams.remaining.headOption match {
        case Some(param) => Failure(new NoSuchParamException(param))
        case None => Success(indexedPgParams.res)
      }
    }
  }

  private def anyToPgArgument(value: Any): Try[Argument] = traced {
    //TODO document in bind null/None/Some support
    value match {
      case null | None => Success(Argument.Null(Oid.unknownDataType))
      case NullParam(cls) => withPgType(cls)(pgType => Success(Argument.Null(pgType.typeOid)))
      case NotNullParam(notNullVal) => notNullToPgParamValue(notNullVal)
      case Some(notNullVal) => notNullToPgParamValue(notNullVal)
      case notNullVal => notNullToPgParamValue(notNullVal)
    }
  }

  private def notNullToPgParamValue(value: Any): Try[Argument] = traced {
    withPgType(value.getClass) { pgType =>
      pgType.asInstanceOf[PgType[Any]].toPgBinary(value)(sessionParams).map { byteVec =>
        Argument.Binary(byteVec, pgType.typeOid)
      }
    }
  }

  private def withPgType[A, B](cls: Class[A])(body: PgType[A] => Try[B]): Try[B] = {
    pgTypes
      .typeByClass(cls)
      .map(body)
      .getOrElse(Failure(new NoSuitableConverterFoundException(cls)))
  }
}
