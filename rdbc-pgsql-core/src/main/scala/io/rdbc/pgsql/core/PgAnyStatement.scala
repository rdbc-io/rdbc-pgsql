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

import io.rdbc.ImmutIndexedSeq
import io.rdbc.api.exceptions.{MissingParamValException, NoSuitableConverterFoundException}
import io.rdbc.implbase.BindablePartialImpl
import io.rdbc.pgsql.core.messages.data.Unknown
import io.rdbc.pgsql.core.messages.frontend.{BinaryDbValue, DbValue, NullDbValue}
import io.rdbc.pgsql.core.types.{PgType, PgTypeRegistry}
import io.rdbc.sapi._
import org.reactivestreams.Publisher

import scala.concurrent.{ExecutionContext, Future}

class PgAnyStatement(stmtExecutor: PgStatementExecutor,
                     stmtDeallocator: PgStatementDeallocator,
                     pgTypeRegistry: PgTypeRegistry,
                     sessionParams: SessionParams,
                     val nativeStmt: PgNativeStatement)(implicit ec: ExecutionContext)
  extends AnyStatement
    with BindablePartialImpl[AnyParametrizedStatement] {

  def nativeSql: String = nativeStmt.statement

  def bind(params: (String, Any)*): AnyParametrizedStatement = {
    val dbValues: Map[String, DbValue] = convertNamedParams(Map(params: _*))
    val indexedDbValues = nativeStmt.params.foldLeft(Vector.empty[DbValue]) { (acc, paramName) =>
      dbValues.get(paramName) match {
        case Some(paramValue) => acc :+ paramValue
        case None => throw new MissingParamValException(paramName)
      }
    }
    parametrizedStmt(indexedDbValues)
  }

  def bindByIdx(params: Any*): AnyParametrizedStatement = {
    val dbValues = params.map(convertParam).toVector
    parametrizedStmt(dbValues)
  }

  def noParams: AnyParametrizedStatement = parametrizedStmt(Vector.empty) //TODO validate whether there really are no params in the statement

  def streamParams(paramsPublisher: Publisher[Map[String, Any]]): Future[Unit] = {
    //TODO parse first
    /*
        //Parse(stmtName, sql, List.empty)
        //, Bind(stmtName.map(_ + "_portal"), stmtName, params.toList, AllTextual)) //TODO toList, //TODO AllTextual

        //sessionRef.actor ! Parse

        val src: Source[Bind, NotUsed] = Source.fromPublisher(paramsPublisher).map { params =>
          //TODO massive code dupl

          val errorsOrDbValues: ImmutSeq[BindEx Xor (String, DbValue)] = convertParams(params)
          val errorOrDbValues: CompositeBindEx Xor Map[String, DbValue] = foldConvertedParams(errorsOrDbValues)
          val errorOrIndexedDbValues: Xor[BindEx, Vector[DbValue]] = errorOrDbValues.flatMap { providedParams =>
            nativeStmt.params.foldLeft(Xor.right[BindEx, Vector[DbValue]](Vector.empty[DbValue])) { (xorAcc, paramName) =>
              xorAcc.flatMap { acc =>
                providedParams.get(paramName) match {
                  case Some(paramValue) => Xor.right(acc :+ paramValue)
                  case None => Xor.left(MissingParamValEx(paramName))
                }
              }
            }
          }

          val dbParams = errorOrIndexedDbValues.getOrElse(???) //TODO

          Bind(Some("STREAMING_P"), Some("STREAMING_S"), dbParams.toList, AllTextual)
        }

        sessionRef.actor ! SubscribeBinds(src)

        XorF(Xor.right())
        */
    ???
  }

  def deallocate(): Future[Unit] = stmtDeallocator.deallocateStatement(nativeSql)

  private def parametrizedStmt(dbValues: ImmutIndexedSeq[DbValue]): AnyParametrizedStatement = {
    new PgParametrizedStatement(stmtExecutor, stmtDeallocator, nativeStmt.statement, dbValues)
  }

  private def convertParam(value: Any): DbValue = {
    //TODO document in bind null/None/Some support
    value match {
      case null | None => NullDbValue(Unknown.oid)
      case NullParam(cls) => withPgType(cls)(pgType => NullDbValue(pgType.typeOid))
      case NotNullParam(notNullVal) => convertNotNullParam(notNullVal)
      case Some(notNullVal) => convertNotNullParam(notNullVal)
      case notNullVal => convertNotNullParam(notNullVal)
    }
  }

  private def withPgType[A, B](cls: Class[A])(block: PgType[A] => B): B = {
    pgTypeRegistry.byClass(cls)
      .map(block)
      .getOrElse(throw NoSuitableConverterFoundException(cls)) //TODO NoSuitableConverterFound should accept cls, not value
  }

  private def convertNotNullParam(value: Any): DbValue = {
    //TODO make it configurable whether use textual or binary
    withPgType(value.getClass) { pgType =>
      val binVal = pgType.asInstanceOf[PgType[Any]].toPgBinary(value)(sessionParams) //TODO textual vs binary
      BinaryDbValue(binVal, pgType.typeOid)
    }
  }

  private def convertNamedParams(params: Map[String, Any]): Map[String, DbValue] = params.map { nameValue =>
    val (name, value) = nameValue
    (name, convertParam(value))
  }
}