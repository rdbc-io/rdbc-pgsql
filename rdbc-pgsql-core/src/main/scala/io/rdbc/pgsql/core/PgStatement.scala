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
import io.rdbc.api.exceptions.BindException.MissingParamValException
import io.rdbc.api.exceptions.NoSuitableConverterFoundException
import io.rdbc.implbase.BindablePartialImpl
import io.rdbc.pgsql.core.messages.frontend.{BinaryDbValue, DbValue}
import io.rdbc.pgsql.core.types.PgType
import io.rdbc.sapi.{ParametrizedStatement, Statement}
import org.reactivestreams.Publisher

import scala.concurrent.Future

class PgStatement(conn: PgConnection, val nativeStmt: PgNativeStatement)
  extends Statement
    with BindablePartialImpl[ParametrizedStatement] {

  val pgTypeRegistry = conn.pgTypeConvRegistry
  implicit def sessionParams = conn.sessionParams

  private def parametrizedStmt(dbValues: ImmutIndexedSeq[DbValue]): ParametrizedStatement = {
    new PgParametrizedStatement(conn, nativeStmt.statement, dbValues)
  }

  def nativeSql: String = nativeStmt.statement

  def convertParams(params: Map[String, Any]): Map[String, DbValue] = params.map { anyParamEntry =>
    val (anyParamName, anyParamValue) = anyParamEntry
    pgTypeRegistry.byClass(anyParamValue.getClass)
      .map(pgType => {
        val binVal = pgType.asInstanceOf[PgType[Any]].toPgBinary(anyParamValue) //TODO textual vs binary
        (anyParamName, BinaryDbValue(binVal): DbValue)
      })
      .getOrElse(throw NoSuitableConverterFoundException(anyParamValue))
  }


  override def bind(params: (String, Any)*): ParametrizedStatement = {
    //TODO make it configurable whether use textual or binary
    val dbValues: Map[String, DbValue] = convertParams(Map(params: _*))
    val indexedDbValues = nativeStmt.params.foldLeft(Vector.empty[DbValue]) { (acc, paramName) =>
      dbValues.get(paramName) match {
        case Some(paramValue) => acc :+ paramValue
        case None => throw MissingParamValException(paramName)
      }
    }
    parametrizedStmt(indexedDbValues)
  }

  override def bindByIdx(params: Any*): ParametrizedStatement = ???

  override def noParams: ParametrizedStatement = parametrizedStmt(Vector.empty) //TODO validate whether there really are no params in the statement

  override def streamParams(paramsPublisher: Publisher[Map[String, Any]]): Future[Unit] = {
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
}
