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

package io.rdbc.pgsql.core.messages.backend

object StatusMessage {
  def error(fields: Map[Byte, String]): ErrorMessage = {
    ErrorMessage(statusData(fields))
  }

  def notice(fields: Map[Byte, String]): NoticeMessage = {
    NoticeMessage(statusData(fields))
  }


  def statusData(fields: Map[Byte, String]): StatusData = {
    //mandatory
    val severity: Option[String] = fields.get('V').orElse(fields.get('S'))
    //TODO error on missing
    val file: Option[String] = fields.get('F')
    val line: Option[String] = fields.get('L')
    val routine: Option[String] = fields.get('R')
    val sqlState: Option[String] = fields.get('C')

    //conversion errors may happen
    val position: Option[Int] = fields.get('P').map(_.toInt)
    //TODO error?
    val internalPosition: Option[Int] = fields.get('p').map(_.toInt) //TODO error?

    StatusData(
      severity = severity.getOrElse("dupa"),
      sqlState = sqlState.getOrElse("dupa"),
      message = fields.getOrElse('M', "dupa"),
      detail = fields.get('D'),
      hint = fields.get('H'),
      position = position,
      internalPosition = internalPosition,
      internalQuery = fields.get('q'),
      where = fields.get('W'),
      schemaName = fields.get('s'),
      tableName = fields.get('t'),
      columnName = fields.get('c'),
      dataTypeName = fields.get('d'),
      constraintName = fields.get('n'),
      file = file.getOrElse("dupa"),
      line = line.getOrElse("dupa"),
      routine = routine.getOrElse("dupa")
    )
  }
}

case class StatusData(
                       severity: String,
                       sqlState: String,
                       message: String,
                       detail: Option[String],
                       hint: Option[String],
                       position: Option[Int],
                       internalPosition: Option[Int],
                       internalQuery: Option[String],
                       where: Option[String],
                       schemaName: Option[String],
                       tableName: Option[String],
                       columnName: Option[String],
                       dataTypeName: Option[String],
                       constraintName: Option[String],
                       file: String,
                       line: String,
                       routine: String
                     ) {

  def shortInfo: String = s"$severity-$sqlState: $message"

  override def toString: String = {
    s"""
       |severity=$severity
       |sqlState=$sqlState
       |message=$message
       |detail=${detail.getOrElse("none")}
       |hint=${hint.getOrElse("none")}
       |position=${position.map(_.toString).getOrElse("none")}
       |internalPosition=${internalPosition.map(_.toString).getOrElse("none")}
       |internalQuery=${internalQuery.getOrElse("none")}
       |where=${where.getOrElse("none")}
       |schemaName=${schemaName.getOrElse("none")}
       |tableName=${tableName.getOrElse("none")}
       |columnName=${columnName.getOrElse("none")}
       |dataTypeName=${dataTypeName.getOrElse("none")}
       |constraintName=${constraintName.getOrElse("none")}
       |file=$file
       |line=$line
       |routine=$routine
       |""".stripMargin
  }
}

sealed trait StatusMessage extends PgBackendMessage {
  def statusData: StatusData
  def isWarning: Boolean = statusData.sqlState.startsWith("01") || statusData.sqlState.startsWith("02")
}

case class ErrorMessage(statusData: StatusData) extends StatusMessage {
  def isFatal: Boolean = {
    if (statusData.sqlState == "57014") {
      false //query canceled
    } else {
      val errCat = statusData.sqlState.take(2)
      errCat match {
        case "57" => true //operator intervention
        case "58" => true //system error
        case "XX" => true //PG internal error
        case _ => false
      }
    }
  }
}

case class NoticeMessage(statusData: StatusData) extends StatusMessage
