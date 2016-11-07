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

package io.rdbc.pgsql.playground

import io.rdbc.pgsql.transport.netty.NettyPgConnectionFactory
import io.rdbc.sapi.Row
import org.reactivestreams.{Subscriber, Subscription}
import scodec.bits.BitVector

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Random, Success, Try}

object Tst extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder", "povder")

  fact.connection().flatMap { conn =>
    println("hai\n\n\n")

    val rsFut = for {
      stmt <- conn.statement("select * from test where x > :x")
      parametrized <- stmt.bindF("x" -> -100)
      rs <- parametrized.executeForSet()
    } yield rs

    rsFut.map { rs =>
      rs.foreach { row =>
        println(s"x = ${row.int("x")}, t = ${row.localDateTime("t")}, s = ${row.str("s")}")
      }
      println("DONE")
    }.map { _ =>
      conn.release()
      fact.shutdown()
    }

  }.recover {
    case ex => ex.printStackTrace()
  }

}


object NoDataTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder", "povder")

  fact.connection().flatMap { conn =>
    println("hai\n\n\n")

    val rsFut = for {
      stmt <- conn.statement("create table if not exists dupa (x varchar)")
      parametrized <- stmt.noParamsF
      rs <- parametrized.executeForSet()
    } yield rs

    val result = rsFut.map { rs =>
      rs.foreach { row =>
        println(s"x = ${row.int("x")}, t = ${row.localDateTime("t")}, s = ${row.str("s")}")
      }
      println("DONE")
    }

    result.onComplete(_ => conn.release())
    result

  }.recover {
    case ex => ex.printStackTrace()
  }.onComplete(_ => fact.shutdown())

}

object EmptyQueryTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder", "povder")

  fact.connection().flatMap { conn =>
    println("hai\n\n\n")

    val rsFut = for {
      stmt <- conn.statement("")
      parametrized <- stmt.noParamsF
      rs <- parametrized.executeForSet()
    } yield rs

    val result = rsFut.map { rs =>
      rs.foreach { row =>
        println(s"x = ${row.int("x")}, t = ${row.localDateTime("t")}, s = ${row.str("s")}")
      }
      println("DONE")
    }

    result.onComplete(_ => conn.release())
    result

  }.recover {
    case ex => ex.printStackTrace()
  }.onComplete(_ => fact.shutdown())

}

object BindByIdxTst extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder", "povder")

  fact.connection().flatMap { conn =>
    println("hai\n\n\n")

    val rsFut = for {
      stmt <- conn.statement("select * from test where x > :x and x > :y and x > :z")
      parametrized <- stmt.bindByIdxF(-100, -200, -300)
      rs <- parametrized.executeForSet()
    } yield rs

    rsFut.map { rs =>
      rs.foreach { row =>
        println(s"x = ${row.int("x")}, t = ${row.localDateTime("t")}, s = ${row.str("s")}")
      }
      println("DONE")
    }.map { _ =>
      conn.release()
    }

  }.recover {
    case ex => ex.printStackTrace()
  }.onComplete(_ => fact.shutdown())

}

object IdleTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  NettyPgConnectionFactory("localhost", 5432, "povder", "povder", "povder").connection().map { conn =>
    println("hai\n\n\n")


  }.recover {
    case ex => ex.printStackTrace()
  }

}


object TstFloat extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  NettyPgConnectionFactory("localhost", 5432, "povder", "povder", "povder").connection().flatMap { conn =>
    println("hai\n\n\n")

    val rsFut = for {
      stmt <- conn.statement("select * from test3")
      parametrized <- stmt.noParamsF
      rs <- parametrized.executeForSet()
    } yield rs

    rsFut.map { rs =>
      rs.foreach { row =>
        println(s"f = ${row.float("f")}, x = ${row.int("x")}, t = ${row.localDateTime("t")}, s = ${row.str("s")}")
      }
      println("DONE")
    }.map(_ => conn.release())

  }.recover {
    case ex => ex.printStackTrace()
  }

}


class SlowSubscriber extends Subscriber[Row] {
  override def onError(t: Throwable): Unit = t.printStackTrace()

  override def onSubscribe(s: Subscription): Unit = {
    new Thread() {
      override def run(): Unit = {
        var i = 0
        var cancelled = false
        while (!cancelled) {
          s.request(5L)
          Thread.sleep(1000L)
          i += 1
          if (i == 2) {
            println("cancelling")
            s.cancel()
            cancelled = true
          }
        }
      }
    }.run()
  }
  override def onComplete(): Unit = println("complete")

  override def onNext(t: Row): Unit = println(s"next $t")
}


object StreamTest extends App {
  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  NettyPgConnectionFactory("localhost", 5432, "povder", "povder", "povder").connection().flatMap { conn =>
    println("hai\n\n\n")

    val streamFut = conn.statement("select * from test").flatMap { stmt =>
      stmt.noParamsF.flatMap { parametrized =>
        parametrized.executeForStream()
      }
    }

    streamFut.map { stream =>
      stream.rows.subscribe(new SlowSubscriber())
      println("DONE")
    }

  }.recover {
    case ex => ex.printStackTrace()
  }
}

object TxTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  NettyPgConnectionFactory("localhost", 5432, "povder", "povder", "povder").connection().flatMap { conn =>
    println("hai\n\n\n")

    val rsFut = for {
      _ <- conn.beginTx()
      stmt <- conn.statement("select * from test where x = :x")
      parametrized <- stmt.bindF("x" -> 421)
      rs <- parametrized.executeForSet()
      _ <- conn.rollbackTx()
    } yield rs

    rsFut.map { rs =>
      println(s"rs = $rs")
      println("DONE")
    }

  }.recover {
    case ex => ex.printStackTrace()
  }
}

object ErrTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  NettyPgConnectionFactory("localhost", 5432, "povder", "povder", "povder").connection().flatMap { conn =>
    println("hai\n\n\n")

    val rsFut = for {
      stmt <- conn.statement("select * from tes5")
      parametrized <- stmt.noParamsF
      rs <- parametrized.executeForSet()
    } yield rs

    rsFut.map { rs =>
      println(s"rs = $rs")
      println("DONE")
    }

  }.recover {
    case ex => ex.printStackTrace()
  }

}

object PerfTst extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val connFact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder", "povder")

  connFact.connection().flatMap { conn =>
    println("hai\n\n\n")

    (1 to 100).foreach { i =>

      val start = System.nanoTime()

      val rsFut = for {
        stmt <- conn.statement("select * from test")
        parametrized <- stmt.noParamsF
        rs <- parametrized.executeForSet()
      } yield rs

      rsFut.map { rs =>
        val time = System.nanoTime() - start
        println("time = " + (time / 1000000.0) + "ms")
      }
      Await.ready(rsFut, Duration.Inf)
    }

    conn.release()

  }.onComplete {
    case Success(_) =>
      connFact.shutdown()
      println("ok")

    case Failure(ex) => ex.printStackTrace()
  }

}

object DecodeTst extends App {

  def hex2bytes(hex: String): Array[Byte] = {
    hex.sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte)
  }

  val bytes = hex2bytes("ffffe35173ec2000")

  val x = scodec.codecs.long(64).decodeValue(BitVector.apply(bytes))

  println(x)

  val l: Long = ((bytes(0) & 255) << 56).toLong +
    ((bytes(1) & 255) << 48).toLong +
    ((bytes(2) & 255) << 40).toLong +
    ((bytes(3) & 255) << 32).toLong +
    ((bytes(4) & 255) << 24).toLong +
    ((bytes(5) & 255) << 16).toLong +
    ((bytes(6) & 255) << 8).toLong +
    (bytes(7) & 255)

  println(l)

}

object TimeoutTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(5, "seconds")

  NettyPgConnectionFactory("localhost", 5432, "povder", "povder", "povder").connection().flatMap { conn =>
    println("hai\n\n\n")

    val rsFut = for {
      stmt <- conn.statement("select pg_sleep(5)")
      parametrized <- stmt.noParamsF
      rs <- parametrized.executeForSet()
    } yield rs

    rsFut.map { rs =>
      println(s"rs = $rs")
      println("DONE")
    }

  }.recover {
    case ex => ex.printStackTrace()
  }

}

object ConnClosedTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  NettyPgConnectionFactory("localhost", 5432, "povder", "povder", "povder").connection().flatMap { conn =>
    println("hai\n\n\n")

    val rsFut = for {
      stmt <- conn.statement("select pg_sleep(60)")
      parametrized <- stmt.noParamsF
      rs <- parametrized.executeForSet()
    } yield rs

    rsFut.map { rs =>
      println(s"rs = $rs")
      println("DONE")
    }.recover {
      case ex => println(ex.getMessage)
    }

    rsFut.andThen { case _ =>
      val rsFut2 = for {
        stmt <- conn.statement("select 1")
        parametrized <- stmt.noParamsF
        rs <- parametrized.executeForSet()
      } yield rs

      rsFut2.map { rs =>
        println("RS2 done")
      }.recover {
        case ex =>
          println("RS2 failed")
          ex.printStackTrace()
      }
    }


  }.recover {
    case ex => ex.printStackTrace()
  }

}

object IfIdleTest extends App {
  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  NettyPgConnectionFactory("localhost", 5432, "povder", "povder", "povder").connection().map { conn =>
    println("hai\n\n\n")

    val rsFut = for {
      stmt <- conn.statement("select pg_sleep(60)")
      parametrized <- stmt.noParamsF
      rs <- parametrized.executeForSet() //TODO executeForSet may use a different thread internally - should I allow this?
    } yield rs


    rsFut.onComplete(r => println("fut1 = " + r))

    val rsFut2 = for {//TODO will this work without synchronization? Client can execute from multiple threads - should I allow this?
      stmt <- conn.statement("select pg_sleep(60)")
      parametrized <- stmt.noParamsF
      rs <- parametrized.executeForSet() //this should fail because of illegal state
    } yield rs


    rsFut2.onComplete(r => println("fut2 = " + r))


  }.recover {
    case ex => ex.printStackTrace()
  }
}

object ParallelTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val connFact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder", "povder")

  connFact.connection().flatMap { conn =>
    println("hai\n\n\n")

    var f = Future.successful(())

    (1 to 10).foreach { i =>
      println(s"starting $i")

      val start = System.nanoTime()

      val rsFut = for {
        stmt <- conn.statement(s"select pg_sleep(${Random.nextInt(2)}) from test")
        parametrized <- stmt.noParamsF
        rs <- parametrized.executeForSet()
      } yield rs

      val fut = rsFut.map { rs =>
        val time = System.nanoTime() - start
        println(s"$i time = ${time / 1000000.0}ms")
      }.recover {
        case ex => println(s"$i err ${ex.getMessage}")
      }
      Thread.sleep(Random.nextInt(700))
      f = fut
    }

    f.flatMap { _ =>
      conn.release()
    }.recover {
      case _ => conn.release()
    }

  }.onComplete {
    case Success(_) =>
      connFact.shutdown()
      println("ok")

    case Failure(ex) => ex.printStackTrace()
  }

}

object SmallintTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder", "povder")

  fact.connection().flatMap { conn =>
    println("hai\n\n\n")

    val rsFut = for {
      stmt <- conn.insert("insert into test_smallint(x) values (:x)")
      parametrized <- stmt.bindF("x" -> Int.MaxValue)
      rs <- parametrized.execute()
    } yield rs

    val result = rsFut.map { _ =>
      println("DONE")
    }

    result.onComplete(_ => conn.release())
    result

  }.recover {
    case ex => ex.printStackTrace()
  }.onComplete(_ => fact.shutdown())

}

class TryPartialFunction[-A, +B](delegate: PartialFunction[A, B]) extends PartialFunction[A, B] {
  def isDefinedAt(x: A): Boolean = delegate.isDefinedAt(x)

  def apply(v: A): B = {
    try {
      delegate.apply(v)
    } catch {
      case ex => println("HAI"); ???
    }
  }
}

object PartialFunTest extends App {

  val pf: PartialFunction[String, String] = {
    case "dupa" => "penis"
  }


  pf.apply("zło")

}
