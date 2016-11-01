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

import io.rdbc._

object PgNativeStatement {
  def parse(statement: String): PgNativeStatement = {
    /* matches ":param1", ":param2" etc. and groups a match without an initial colon */
    val paramPattern = """:([^\W]*)""".r
    val params = paramPattern.findAllMatchIn(statement).map(_.group(1)).toVector
    val nativeStatement = params.zipWithIndex.foldLeft(statement) { (acc, elem) =>
      val (param, idx) = elem
      acc.replaceFirst(":" + param, "\\$" + (idx + 1))
    }
    PgNativeStatement(nativeStatement, params)
  }
}

case class PgNativeStatement(statement: String, params: ImmutIndexedSeq[String])
