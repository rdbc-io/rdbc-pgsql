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

package io.rdbc.pgsql.core.internal

import io.rdbc._
import io.rdbc.pgsql.core.RdbcSql
import io.rdbc.pgsql.core.pgstruct.messages.frontend.NativeSql
import io.rdbc.util.Logging

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

private[core] object PgNativeStatement extends Logging {

  private val cast = """::[a-zA-Z]\w*"""
  private val sqlString = "'.+?'"
  private val column = """".+?""""
  private val blockComment = """/\*.*?\*/"""
  private val lineComment = "--.*"
  private val param = """(:[a-zA-Z]\w*)"""
  //TODO support dollar quoting?
  //TODO test newlines

  private[this] val pattern = new Regex(
    s"$sqlString|$column|$blockComment|$lineComment|$cast|$param"
  )

  def parse(statement: RdbcSql): PgNativeStatement = traced {
    val sb = new StringBuilder
    val params = ListBuffer.empty[String]
    var lastTextIdx = 0
    var lastParamIdx = 0
    pattern.findAllMatchIn(statement.value).filter(_.group(1) != null).foreach { m =>
      sb.append(statement.value.substring(lastTextIdx, m.start))
      lastParamIdx += 1
      sb.append("$").append(lastParamIdx)
      params += m.group(1).drop(1)
      lastTextIdx = m.end
    }
    sb.append(statement.value.substring(lastTextIdx))

    PgNativeStatement(NativeSql(sb.toString), params.toVector)
  }
}

private[core] case class PgNativeStatement(sql: NativeSql, params: ImmutIndexedSeq[String])
