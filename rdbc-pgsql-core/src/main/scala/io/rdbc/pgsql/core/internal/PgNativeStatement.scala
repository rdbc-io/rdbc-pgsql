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

import io.rdbc._
import io.rdbc.api.exceptions.MixedParamTypesException
import io.rdbc.pgsql.core.internal.PgNativeStatement.Params
import io.rdbc.pgsql.core.pgstruct.messages.frontend.NativeSql
import io.rdbc.util.Logging

import scala.util.matching.Regex

private[core] object PgNativeStatement extends Logging {

  sealed trait Params
  object Params {
    case class Named(names: ImmutIndexedSeq[String]) extends Params
    case class Positional(count: Int) extends Params
  }

  private case class ParamMatch(value: String, start: Int, end: Int)
  private case class ParamMatches(named: Vector[ParamMatch],
                                  positional: Vector[ParamMatch])


  private val cast = """::[a-zA-Z]\w*"""
  private val sqlString = "'.+?'"
  private val column = """".+?""""
  private val blockComment = """/\*.*?\*/"""
  private val lineComment = "--.*"
  private val namedParam = """(:[a-zA-Z]\w*)"""
  private val positionalParam = "(\\?)"
  //TODO support dollar quoting?
  //TODO test newlines'

  private[this] val pattern = new Regex(
    s"$sqlString|$column|$blockComment|$lineComment|$cast|$namedParam|$positionalParam"
  )

  private val namedGrp = 1
  private val positionalGrp = 2

  private def findParamMatches(statement: RdbcSql): ParamMatches = {

    def regexMatch2ParamMatch(m: Regex.Match, grp: Int): ParamMatch = {
      ParamMatch(m.group(grp), m.start, m.end)
    }

    val matches = pattern.findAllMatchIn(statement.value).toTraversable
    val namedMatches = matches.filter(_.group(namedGrp) != null)
    val positionalMatches = matches.filter(_.group(positionalGrp) != null)
    ParamMatches(
      named = namedMatches.map(regexMatch2ParamMatch(_, namedGrp)).toVector,
      positional = positionalMatches.map(regexMatch2ParamMatch(_, positionalGrp)).toVector
    )
  }

  def parse(statement: RdbcSql): PgNativeStatement = traced {
    val matches = findParamMatches(statement)
    if (matches.named.nonEmpty && matches.positional.nonEmpty) {
      throw new MixedParamTypesException
    } else {
      if (matches.named.nonEmpty) {
        val params = Params.Named(matches.named.map(_.value.drop(1)))
        PgNativeStatement(toNativeSql(statement, matches.named), params)
      } else {
        val params = Params.Positional(matches.positional.size)
        PgNativeStatement(toNativeSql(statement, matches.positional), params)
      }
    }
  }

  private def toNativeSql(statement: RdbcSql, matches: Vector[ParamMatch]): NativeSql = {
    val sb = new StringBuilder
    var lastTextIdx = 0
    var lastParamIdx = 0
    matches.foreach { m =>
      sb.append(statement.value.substring(lastTextIdx, m.start))
      lastParamIdx += 1
      sb.append("$").append(lastParamIdx)
      lastTextIdx = m.end
    }
    sb.append(statement.value.substring(lastTextIdx))
    NativeSql(sb.toString())
  }
}

private[core] case class PgNativeStatement(sql: NativeSql, params: Params)
