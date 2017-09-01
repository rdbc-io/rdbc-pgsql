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

package io.rdbc.pgsql.core

import io.rdbc.api.exceptions.MixedParamTypesException
import io.rdbc.pgsql.core.internal.PgNativeStatement.Params
import io.rdbc.pgsql.core.internal.{PgNativeStatement, RdbcSql}
import io.rdbc.pgsql.core.pgstruct.messages.frontend.NativeSql
import org.scalatest.Inside

class PgNativeStatementSpec
  extends RdbcPgsqlCoreSpec
    with Inside {

  "PgNativeStatement" should {
    "parse statement without params" in {
      val sql = "select * from table where col = 1"
      val stmt = parse(sql)

      stmt.sql shouldBe NativeSql(sql)
      stmt.params shouldBe 'empty
    }

    "parse statement with positional params" when {
      "parameters are at the beginning, in the middle and at the end" in {
        val sql = "? select ? from table where col1 = ? and col2 = ?"
        val stmt = parse(sql)

        stmt.sql shouldBe NativeSql("$1 select $2 from table where col1 = $3 and col2 = $4")
        stmt.params shouldBe Params.Positional(4)
      }

      "parameters are separated with operators" in {
        val sql = "select ?-? from table where ?+?=?"
        val stmt = parse(sql)

        stmt.sql shouldBe NativeSql("select $1-$2 from table where $3+$4=$5")
        stmt.params shouldBe Params.Positional(5)
      }

      "parameters are adjacent to parentheses and commas" in {
        val sql = "insert into tbl values(?,?,?)"
        val stmt = parse(sql)

        stmt.sql shouldBe NativeSql("insert into tbl values($1,$2,$3)")
        stmt.params shouldBe Params.Positional(3)
      }
    }

    "parse statement with named params" when {
      "parameters are at the beginning, in the middle and at the end" in {
        val sql = ":p1 select :p2 from table where col1 = :p3 and col2 = :p4"
        val stmt = parse(sql)

        stmt.sql shouldBe NativeSql("$1 select $2 from table where col1 = $3 and col2 = $4")
        stmt.params shouldBe Params.Named(Vector("p1", "p2", "p3", "p4"))
      }

      "parameters are separated with operators" in {
        val sql = "select :p1-:p2 from table where :p3+:p4=:p5"
        val stmt = parse(sql)

        stmt.sql shouldBe NativeSql("select $1-$2 from table where $3+$4=$5")
        stmt.params shouldBe Params.Named(Vector("p1", "p2", "p3", "p4", "p5"))
      }

      "parameters are adjacent to parentheses and commas" in {
        val sql = "insert into tbl values(:p1,:p2,:p3)"
        val stmt = parse(sql)

        stmt.sql shouldBe NativeSql("insert into tbl values($1,$2,$3)")
        stmt.params shouldBe Params.Named(Vector("p1", "p2", "p3"))
      }

      "named parameter is used in multiple places" in {
        val sql = "select * from table where col1 = :p1 and col2 = :p2 and col3 = :p1"
        val stmt = parse(sql)

        stmt.sql shouldBe NativeSql("select * from table where col1 = $1 and col2 = $2 and col3 = $3")
        stmt.params shouldBe Params.Named(Vector("p1", "p2", "p1"))
      }
    }

    "handle only named params that are alphanum identifiers starting with a letter" in {
      val sql = "select * from table where col1 = :1p1 and col2 = :p2 and col3 = :(p3"
      val stmt = parse(sql)

      stmt.sql shouldBe NativeSql("select * from table where col1 = :1p1 and col2 = $1 and col3 = :(p3")
      stmt.params shouldBe Params.Named(Vector("p2"))
    }

    "ignore positional param" when {
      "it's in a varchar literal" in {
        val sql = "'?' select * from table where col1 = '?' and col2 = ? and col3 = '?'"
        val stmt = parse(sql)

        stmt.sql shouldBe NativeSql("'?' select * from table where col1 = '?' and col2 = $1 and col3 = '?'")
        stmt.params shouldBe Params.Positional(1)
      }

      "it's in a column alias" in {
        val sql = """"?" select x as "?" from table where col1 = "?" and col2 = ? and col3 = "?""""
        val stmt = parse(sql)

        stmt.sql shouldBe NativeSql(""""?" select x as "?" from table where col1 = "?" and col2 = $1 and col3 = "?"""")
        stmt.params shouldBe Params.Positional(1)
      }

      "it's in a block comment" in {
        val sql = "/*?*/ select x from table where col1 = /* ? */ and col2 = ? and col3 = /* ?*/"
        val stmt = parse(sql)

        stmt.sql shouldBe NativeSql("/*?*/ select x from table where col1 = /* ? */ and col2 = $1 and col3 = /* ?*/")
        stmt.params shouldBe Params.Positional(1)
      }

      "it's in a line comment" when {
        "there's no space at the beginning of the comment" in {
          val sql = "select x from table where col1 = ? --and col2 = ? and col3 = ?"
          val stmt = parse(sql)

          stmt.sql shouldBe NativeSql("select x from table where col1 = $1 --and col2 = ? and col3 = ?")
          stmt.params shouldBe Params.Positional(1)
        }

        "there's a space at the beginning of the comment" in {
          val sql = "select x from table where col1 = ? -- and col2 = ? and col3 = ?"
          val stmt = parse(sql)

          stmt.sql shouldBe NativeSql("select x from table where col1 = $1 -- and col2 = ? and col3 = ?")
          stmt.params shouldBe Params.Positional(1)
        }

        "there's no space before -- starting the comment" in {
          val sql = "select x from table where col1 = ?-- and col2 = ? and col3 = ?"
          val stmt = parse(sql)

          stmt.sql shouldBe NativeSql("select x from table where col1 = $1-- and col2 = ? and col3 = ?")
          stmt.params shouldBe Params.Positional(1)
        }
      }
    }

    "ignore named param" when {
      "it's in a varchar literal" in {
        val sql = "':p1' select * from table where col1 = ':p2' and col2 = :p3 and col3 = ':p4'"
        val stmt = parse(sql)

        stmt.sql shouldBe NativeSql("':p1' select * from table where col1 = ':p2' and col2 = $1 and col3 = ':p4'")
        stmt.params shouldBe Params.Named(Vector("p3"))
      }

      "it's in a column alias" in {
        val sql = """":p1" select x as ":p2" from table where col1 = ":p3" and col2 = :p4 and col3 = ":p5""""
        val stmt = parse(sql)

        stmt.sql shouldBe NativeSql(
          """":p1" select x as ":p2" from table where col1 = ":p3" and col2 = $1 and col3 = ":p5""""
        )
        stmt.params shouldBe Params.Named(Vector("p4"))
      }

      "there is a double colon cast expression" in {
        val sql = "select 1::int, x::time, 1 ::int, x ::time, :p1"
        val stmt = parse(sql)

        stmt.sql shouldBe NativeSql("select 1::int, x::time, 1 ::int, x ::time, $1")
        stmt.params shouldBe Params.Named(Vector("p1"))
      }

      "it's in a block comment" in {
        val sql = "/*:p1*/ select x from table where col1 = /* :p2 */ and col2 = :p3 and col3 = /* :p4*/"
        val stmt = parse(sql)

        stmt.sql shouldBe NativeSql(
          "/*:p1*/ select x from table where col1 = /* :p2 */ and col2 = $1 and col3 = /* :p4*/"
        )
        stmt.params shouldBe Params.Named(Vector("p3"))
      }

      "it's in a line comment" when {
        "there's no space at the beginning of the comment" in {
          val sql = "select x from table where col1 = :p1 --and col2 = :p2 and col3 = :p3"
          val stmt = parse(sql)

          stmt.sql shouldBe NativeSql("select x from table where col1 = $1 --and col2 = :p2 and col3 = :p3")
          stmt.params shouldBe Params.Named(Vector("p1"))
        }

        "there's a space at the beginning of the comment" in {
          val sql = "select x from table where col1 = :p1 -- and col2 = :p2 and col3 = :p3"
          val stmt = parse(sql)

          stmt.sql shouldBe NativeSql("select x from table where col1 = $1 -- and col2 = :p2 and col3 = :p3")
          stmt.params shouldBe Params.Named(Vector("p1"))
        }

        "there's no space before -- starting the comment" in {
          val sql = "select x from table where col1 = :p1-- and col2 = :p2 and col3 = :p3"
          val stmt = parse(sql)

          stmt.sql shouldBe NativeSql("select x from table where col1 = $1-- and col2 = :p2 and col3 = :p3")
          stmt.params shouldBe Params.Named(Vector("p1"))
        }
      }
    }

    "fail when positional and named params are mixed" in {
      val sql = "select * from table where col1 = :named and col2 = ?"
      a[MixedParamTypesException] should be thrownBy {
        parse(sql)
      }
    }
  }

  def parse(sql: String): PgNativeStatement = {
    PgNativeStatement.parse(RdbcSql(sql)).get
  }
}
