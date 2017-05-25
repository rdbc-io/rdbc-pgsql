/*
 * Copyright 2016-2017 Krzysztof Pado
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

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.rdbc.ImmutIndexedSeq
import io.rdbc.api.exceptions._
import io.rdbc.pgsql.core.SessionParams
import io.rdbc.pgsql.core.internal.PgNativeStatement.Params
import io.rdbc.pgsql.core.internal.PgNativeStatement.Params.{Named, Positional}
import io.rdbc.pgsql.core.pgstruct.{Argument, Oid}
import io.rdbc.pgsql.core.types.{PgType, PgTypeRegistry}
import io.rdbc.sapi._
import io.rdbc.util.Logging
import org.reactivestreams.Publisher

import scala.concurrent.{ExecutionContext, Future}

private[core] class PgStatement(stmtExecutor: PgStatementExecutor,
                                pgTypes: PgTypeRegistry,
                                sessionParams: SessionParams,
                                nativeStmt: PgNativeStatement)
                               (implicit ec: ExecutionContext)
  extends Statement
    with Logging {

  //TODO try to limit exceptions to public methods only - scan the code

  def bind(args: (String, Any)*): ExecutableStatement = traced {
    nativeStmt.params match {
      case Positional(_) =>
        args.headOption match {
          case Some((param, _)) => throw new NoSuchParamException(param)
          case None => pgParametrizedStatement(Vector.empty)
        }

      case namedParams: Named =>
        val pgParamValues = argMapToPgArguments(Map(args: _*), namedParams)
        pgParametrizedStatement(pgParamValues)
    }
  }

  def bindByIdx(args: Any*): ExecutableStatement = traced {
    val size = nativeStmt.params match {
      case Positional(count) => count
      case Named(seq) => seq.size
    }
    if (args.size < size) {
      nativeStmt.params match {
        case Positional(_) => throw new MissingParamValException((args.size + 1).toString)
        case Named(seq) => throw new MissingParamValException(seq(args.size))
      }
    } else if (args.size > size) {
      throw new TooManyParamsException(provided = args.size, expected = size)
    } else {
      val pgArguments = args.map(toPgArgument).toVector
      pgParametrizedStatement(pgArguments)
    }
  }

  def noArgs: ExecutableStatement = traced(bindByIdx())

  def streamArgs(argsPublisher: Publisher[Map[String, Any]]): Future[Unit] = traced {
    val pgArgsSource = Source.fromPublisher(argsPublisher).map { argMap =>
      nativeStmt.params match {
        case Positional(_) =>
          argMap.headOption match {
            case Some((param, _)) => throw new NoSuchParamException(param)
            case None => Vector.empty
          }

        case namedParams: Named => argMapToPgArguments(argMap, namedParams)
      }
    }
    streamPgArguments(pgArgsSource)
  }

  def streamArgsByIdx(argsPublisher: Publisher[ImmutIndexedSeq[Any]]): Future[Unit] = traced {
    val pgArgsSource = Source.fromPublisher(argsPublisher).map { argsSeq =>
      argsSeq.map(toPgArgument).toVector
    }
    streamPgArguments(pgArgsSource)
  }

  private def streamPgArguments(argsSource: Source[Vector[Argument], NotUsed]): Future[Unit] = traced {
    stmtExecutor.executeArgsStream(nativeStmt.sql, argsSource)
  }

  private def pgParametrizedStatement(pgParamValues: Vector[Argument]): PgExecutableStatement = traced {
    new PgExecutableStatement(
      executor = stmtExecutor,
      nativeSql = nativeStmt.sql,
      params = pgParamValues
    )
  }

  private def argMapToPgArguments(namedArgs: Map[String, Any], namedParams: Params.Named): Vector[Argument] = traced {
    case class Acc(res: Vector[Argument], remaining: Set[String])

    val Named(params) = namedParams
    val pgParamsMap = namedArgs.mapValues(toPgArgument)
    val init = Acc(res = Vector.empty, remaining = namedArgs.keySet)
    val indexedPgParams = params.foldLeft(init) { (acc, paramName) =>
      acc.copy(
        res = acc.res :+ pgParamsMap.getOrElse(paramName, throw new MissingParamValException(paramName)),
        remaining = acc.remaining - paramName
      )
    }
    indexedPgParams.remaining.headOption match {
      case Some(param) => throw new NoSuchParamException(param)
      case None => indexedPgParams.res
    }
  }

  private def toPgArgument(value: Any): Argument = traced {
    //TODO document in bind null/None/Some support
    value match {
      case null | None => Argument.Null(Oid.unknownDataType)
      case NullParam(cls) => withPgType(cls)(pgType => Argument.Null(pgType.typeOid))
      case NotNullParam(notNullVal) => notNullToPgParamValue(notNullVal)
      case Some(notNullVal) => notNullToPgParamValue(notNullVal)
      case notNullVal => notNullToPgParamValue(notNullVal)
    }
  }

  private def notNullToPgParamValue(value: Any): Argument = traced {
    withPgType(value.getClass) { pgType =>
      val binVal = pgType.asInstanceOf[PgType[Any]].toPgBinary(value)(sessionParams)
      Argument.Binary(binVal, pgType.typeOid)
    }
  }

  private def withPgType[A, B](cls: Class[A])(body: PgType[A] => B): B = {
    pgTypes
      .typeByClass(cls)
      .map(body)
      .getOrElse(throw new NoSuitableConverterFoundException(cls))
  }
}
