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

import io.rdbc.pgsql.core.pgstruct.Oid
import io.rdbc.pgsql.core.types.{PgType, PgTypeRegistry}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Inside

class PgTypeRegistrySpec
  extends RdbcPgsqlCoreSpec
    with Inside
    with MockFactory {

  "PgTypeRegistry" should {
    "return type by class if it exists" when {
      "only one class maps to given type" in {
        val intType = pgType(Oid(10L), classOf[Int], otherClasses = Vector.empty)
        val strType = pgType(Oid(20L), classOf[String], otherClasses = Vector.empty)
        val reg = PgTypeRegistry(intType, strType)

        inside(reg.typeByClass(classOf[Int])) { case Some(res) =>
          res shouldBe theSameInstanceAs(intType)
        }

        inside(reg.typeByClass(classOf[String])) { case Some(res) =>
          res shouldBe theSameInstanceAs(strType)
        }
      }

      "multiple classes map to given type" in {
        val intType = pgType(Oid(10L), classOf[Int], otherClasses = Vector(classOf[Double], classOf[Float]))
        val strType = pgType(Oid(20L), classOf[String], otherClasses = Vector(classOf[Short], classOf[Long]))
        val reg = PgTypeRegistry(intType, strType)

        inside(reg.typeByClass(classOf[Int])) { case Some(res) =>
          res shouldBe theSameInstanceAs(intType)
        }

        inside(reg.typeByClass(classOf[Double])) { case Some(res) =>
          res shouldBe theSameInstanceAs(intType)
        }

        inside(reg.typeByClass(classOf[Float])) { case Some(res) =>
          res shouldBe theSameInstanceAs(intType)
        }

        inside(reg.typeByClass(classOf[String])) { case Some(res) =>
          res shouldBe theSameInstanceAs(strType)
        }

        inside(reg.typeByClass(classOf[Short])) { case Some(res) =>
          res shouldBe theSameInstanceAs(strType)
        }

        inside(reg.typeByClass(classOf[Long])) { case Some(res) =>
          res shouldBe theSameInstanceAs(strType)
        }
      }
    }

    "return type by OID if it exists" in {
      val intOid = Oid(10L)
      val intType = pgType(intOid, classOf[Int], otherClasses = Vector.empty)

      val strOid = Oid(20L)
      val strType = pgType(strOid, classOf[String], otherClasses = Vector.empty)
      val reg = PgTypeRegistry(intType, strType)

      inside(reg.typeByOid(intOid)) { case Some(res) =>
        res shouldBe theSameInstanceAs(intType)
      }

      inside(reg.typeByOid(strOid)) { case Some(res) =>
        res shouldBe theSameInstanceAs(strType)
      }
    }

    "return None when no OID mapping exists" when {
      "registry is empty" in {
        val reg = PgTypeRegistry()
        reg.typeByOid(Oid(10L)) shouldBe empty
      }

      "registry is not empty" in {
        val reg = PgTypeRegistry(pgType(Oid(10L), classOf[String], otherClasses = Vector.empty))
        reg.typeByOid(Oid(20L)) shouldBe empty
      }
    }

    "return None when no class mapping exists" when {
      "registry is empty" in {
        val reg = PgTypeRegistry()
        reg.typeByClass(classOf[Int]) shouldBe empty
      }

      "registry is not empty" in {
        val reg = PgTypeRegistry(pgType(Oid(10L), classOf[String], otherClasses = Vector.empty))
        reg.typeByClass(classOf[Int]) shouldBe empty
      }
    }
  }

  def pgType[A](oid: Oid, cls: Class[A], otherClasses: Vector[Class[_]]): PgType[A] = {
    val t = mock[PgType[A]]
    (t.typeOid _).expects().returning(oid)
    (t.cls _).expects().returning(cls)
    (t.otherClasses _).expects().returning(otherClasses)
    t
  }
}
