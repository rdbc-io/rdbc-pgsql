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

import java.nio.charset.Charset

import akka.stream.scaladsl.{Sink, Source}
import io.rdbc.pgsql.scodec.ScodecDecoderFactory
import io.rdbc.pgsql.transport.netty.NettyPgConnectionFactory
import io.rdbc.sapi.Interpolators._
import io.rdbc.sapi.Row
import org.reactivestreams.{Subscriber, Subscription}
import scodec.bits.BitVector

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Random, Success}

object Tst extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder")

  fact
    .connection()
    .flatMap { conn =>
      println("hai\n\n\n")
      //Thread.sleep(20000L)
      val start = System.nanoTime()

      val x = -100

      val rsFut = for {
        stmt <- conn.statement(sql"SELECT * FROM test WHERE x > $x")
        rs <- stmt.executeForSet()
      //TODO when i ctrl-c postgresql during pulling rows nothing happens
      } yield (stmt, rs)

      rsFut.flatMap {
        case (stmt, rs) =>
          val time = System.nanoTime() - start
          println(s"time = ${time / 1000000}")

          rs.foreach { row =>
            println(s"x = ${row.int("x")}, t = ${row.localDateTime("t")}, s = ${row.str("s")}")
          }
          println("DONE")
          stmt.deallocate()
      }.map { _ =>
        conn.release()
      }

    }
    .recover {
      case ex =>
        println("ERROR")
        ex.printStackTrace()
    }
    .flatMap { _ =>
      println("hai shutdown")
      fact.shutdown()
    }
    .map { _ =>
      println("SHUT DOWN")
    }

}

object CacheTst extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder")

  fact
    .connection()
    .flatMap { conn =>
      println("hai\n\n\n")
      //Thread.sleep(20000L)
      val start = System.nanoTime()

      val x = -100

      val rsFut = for {
        stmt1 <- conn.statement(sql"SELECT * FROM test WHERE x > $x")
        rs1 <- stmt1.executeForSet()
        stmt2 <- conn.statement(sql"SELECT * FROM test WHERE x > $x")
        rs2 <- stmt2.executeForSet()
        stmt3 <- conn.statement(sql"SELECT * FROM test WHERE x > $x")
        rs3 <- stmt3.executeForSet()
      //TODO when i ctrl-c postgresql during pulling rows nothing happens
      } yield (stmt3, rs3)

      rsFut.flatMap {
        case (stmt, rs) =>
          val time = System.nanoTime() - start
          println(s"time = ${time / 1000000}")

          rs.foreach { row =>
            println(s"x = ${row.int("x")}, t = ${row.localDateTime("t")}, s = ${row.str("s")}")
          }
          println("DONE")
          stmt.deallocate()
      }.map { _ =>
        conn.release()
      }

    }
    .recover {
      case ex =>
        println("ERROR")
        ex.printStackTrace()
    }
    .flatMap { _ =>
      println("hai shutdown")
      fact.shutdown()
    }
    .map { _ =>
      println("SHUT DOWN")
    }

}

object InsertTst extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder")

  fact
    .connection()
    .flatMap { conn =>
      println("hai\n\n\n")

      val rsFut = for {
        stmt <- conn.statement("insert into test(x) (select x from test)")
        parametrized <- stmt.noParamsF
        count <- parametrized.executeForRowsAffected()
      } yield (stmt, count)

      rsFut.map {
        case (stmt, count) =>
          println(s"inserted $count")
          println("DONE")
          conn.release()
          fact.shutdown()
      }

    }
    .recover {
      case ex => ex.printStackTrace()
    }

}

object NoDataTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder")

  fact
    .connection()
    .flatMap { conn =>
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

    }
    .recover {
      case ex => ex.printStackTrace()
    }
    .onComplete(_ => fact.shutdown())

}

object EmptyQueryTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder")

  fact
    .connection()
    .flatMap { conn =>
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

    }
    .recover {
      case ex => ex.printStackTrace()
    }
    .onComplete(_ => fact.shutdown())

}

object BindByIdxTst extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder")

  fact
    .connection()
    .flatMap { conn =>
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

    }
    .recover {
      case ex => ex.printStackTrace()
    }
    .onComplete(_ => fact.shutdown())

}

object IdleTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  NettyPgConnectionFactory("localhost", 5432, "povder", "povder")
    .connection()
    .map { conn =>
      println("hai\n\n\n")

    }
    .recover {
      case ex => ex.printStackTrace()
    }

}

object BeginTwiceTst extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  NettyPgConnectionFactory("localhost", 5432, "povder", "povder")
    .connection()
    .map { conn =>
      println("hai\n\n\n")

      for (i <- 1 to 100) {
        Await.result(conn.beginTx().flatMap(_ => conn.commitTx()), Duration.Inf)
      }


    }
    .recover {
      case ex => ex.printStackTrace()
    }

}

object TstFloat extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  NettyPgConnectionFactory("localhost", 5432, "povder", "povder")
    .connection()
    .flatMap { conn =>
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

    }
    .recover {
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

  NettyPgConnectionFactory("localhost", 5432, "povder", "povder")
    .connection()
    .flatMap { conn =>
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

    }
    .recover {
      case ex => ex.printStackTrace()
    }
}

object TxTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  NettyPgConnectionFactory("localhost", 5432, "povder", "povder")
    .connection()
    .flatMap { conn =>
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

    }
    .recover {
      case ex => ex.printStackTrace()
    }
}

object ErrTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  NettyPgConnectionFactory("localhost", 5432, "povder", "povder")
    .connection()
    .flatMap { conn =>
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

    }
    .recover {
      case ex => ex.printStackTrace()
    }

}

object ErrTestTx extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  NettyPgConnectionFactory("localhost", 5432, "povder", "povder")
    .connection()
    .flatMap { conn =>
      println("hai\n\n\n")

      val rsFut = for {
        _ <- conn.beginTx()
        stmt <- conn.statement("select * from tes5")
        parametrized <- stmt.noParamsF
        rs <- parametrized.executeForSet()
        _ <- conn.commitTx()
      } yield rs

      rsFut.map { rs =>
        println(s"rs = $rs")
        println("DONE")
      }

    }
    .recover {
      case ex => ex.printStackTrace()
    }

}

object PerfTst extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val connFact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder")

  connFact
    .connection()
    .flatMap { conn =>
      println("hai\n\n\n")

      (1 to 100).foreach { i =>
        val start = System.nanoTime()

        val rsFut = for {
          stmt <- conn.statement("select x from test")
          parametrized <- stmt.noParamsF
          rs <- parametrized.executeForSet()
        } yield rs

        /*rsFut.map { rs =>

        }*/
        Await.ready(rsFut, Duration.Inf)
        val time = System.nanoTime() - start
        println(s"$i time = " + (time / 1000000.0) + "ms")
      }

      conn.release()

    }
    .onComplete {
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

object KeyGenTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder")

  fact
    .connection()
    .flatMap { conn =>
      println("hai\n\n\n")

      val rsFut = for {
        stmt <- conn.returningInsert("insert into serial_test(x) values (:x)")
        parametrized <- stmt.bindF("x" -> 10)
        keys <- parametrized.executeForKeysSet()
      } yield keys

      rsFut.map { keys =>
        println("KEYS")
        keys.rows.foreach { row =>
          println(" id = " + row.int("id"))
        }
        println("DONE")
        conn.release()
        fact.shutdown()
      }

    }
    .recover {
      case ex => ex.printStackTrace()
    }

}

object TimeoutTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(5, "seconds")

  NettyPgConnectionFactory("localhost", 5432, "povder", "povder")
    .connection()
    .flatMap { conn =>
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

    }
    .recover {
      case ex => ex.printStackTrace()
    }

}

object ConnClosedTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  NettyPgConnectionFactory("localhost", 5432, "povder", "povder")
    .connection()
    .flatMap { conn =>
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

      rsFut.andThen {
        case _ =>
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

    }
    .recover {
      case ex => ex.printStackTrace()
    }

}

object IfIdleTest extends App {
  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  NettyPgConnectionFactory("localhost", 5432, "povder", "povder")
    .connection()
    .map { conn =>
      println("hai\n\n\n")

      val rsFut = for {
        stmt <- conn.statement("select pg_sleep(60)")
        parametrized <- stmt.noParamsF
        rs <- parametrized
          .executeForSet() //TODO executeForSet may use a different thread internally - should I allow this?
      } yield rs

      rsFut.onComplete(r => println("fut1 = " + r))

      val rsFut2 =
        for {//TODO will this work without synchronization? Client can execute from multiple threads - should I allow this?
          stmt <- conn.statement("select pg_sleep(60)")
          parametrized <- stmt.noParamsF
          rs <- parametrized.executeForSet() //this should fail because of illegal state
        } yield rs

      rsFut2.onComplete(r => println("fut2 = " + r))

    }
    .recover {
      case ex => ex.printStackTrace()
    }
}

object ParallelTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val connFact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder")

  connFact
    .connection()
    .flatMap { conn =>
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

    }
    .onComplete {
      case Success(_) =>
        connFact.shutdown()
        println("ok")

      case Failure(ex) => ex.printStackTrace()
    }

}

object SmallintTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder")

  fact
    .connection()
    .flatMap { conn =>
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

    }
    .recover {
      case ex => ex.printStackTrace()
    }
    .onComplete(_ => fact.shutdown())

}

object NullTest extends App {

  import io.rdbc.sapi.SqlParam._

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder")

  fact
    .connection()
    .flatMap { conn =>
      println("hai\n\n\n")

      val intOpt = Option.empty[Int]
      val yOpt = Some(10)

      val rsFut = for {
        stmt <- conn.select("select :x as x, :y as y, :z as z")
        parametrized <- stmt.bindF("x" -> intOpt.toSqlParam, "y" -> yOpt.toSqlParam, "z" -> Some(1))
        rs <- parametrized.executeForSet()
      } yield rs

      val result = rsFut.map { rs =>
        rs.foreach { row =>
          println(s"x = ${row.col[String]("x")}, y = ${row.col[Int]("y")}, z = ${row.col[Int]("z")}")
        }
        println("DONE")
      }

      result.onComplete(_ => conn.release())
      result

    }
    .recover {
      case ex => ex.printStackTrace()
    }
    .onComplete(_ => fact.shutdown())

}

object NullTest2 extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder")

  fact
    .connection()
    .flatMap { conn =>
      println("hai\n\n\n")

      val rsFut = for {
        stmt <- conn.insert("insert into test(x, s) values(:x, 'dupa')")
        parametrized <- stmt.bindF("x" -> Some(1))
        rs <- parametrized.execute()
      } yield rs

      val result = rsFut.map { _ =>
        println("DONE")
      }

      result.onComplete(_ => conn.release())
      result

    }
    .recover {
      case ex => ex.printStackTrace()
    }
    .onComplete(_ => fact.shutdown())

}

/*
object StmTest extends App {

  import scala.concurrent.stm._

  class Mutable {
    var i: Int = 0
  }

  val mutable = Ref(new Mutable)
  val immutable = Ref(0)

  val latch = new CountDownLatch(1)

  val r = new Runnable {
    override def run(): Unit = {
      latch.await()
      atomic { implicit txn =>
       // println(s"Thread ${Thread.currentThread()} entered atomic block")
        mutable().i = mutable().i + 1
        immutable() = immutable() + 1


        Txn.afterCommit(_ => println(s"Thread ${Thread.currentThread()} committed"))
       // Txn.afterRollback(_ => println(s"Thread ${Thread.currentThread()} rolled back"))

      }
    }
  }

  val threads = (1 to 1000).map(_ => new Thread(r))

  threads.foreach(_.start())

  latch.countDown()

  threads.foreach(_.join())

  println("mutable = " + mutable.single().i)
  println("immutable = " + immutable.single())

}
 */

object StreamParamsTst extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder")

  import akka.actor.ActorSystem
  import akka.stream._

  implicit val system = ActorSystem("tst")
  implicit val materializer = ActorMaterializer()

  fact.connection().foreach { conn =>
    println("hai\n\n\n")

    var i = 0
    while (i < 100) {
      val params = Source(1 to 10000).map(i => Map("x" -> i.asInstanceOf[Any])).runWith(Sink.asPublisher(false))

      val start = System.nanoTime()
      val stmtFut = for {
        _ <- conn.beginTx()
        stmt <- conn.statement("insert into test(x) values (:x)")
        _ <- stmt.streamParams(params)
      } yield stmt

      val c = stmtFut.flatMap(_ => conn.commitTx())

      val fut = c.map { _ =>
        val time = System.nanoTime() - start
        println(s"$i DONE: ${time / 1000000}ms")
      }

      Await.result(fut, Duration.Inf)

      i += 1
    }
  }

}

object ManyInsertTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder")

  fact.connection().foreach { conn =>
    println("hai\n\n\n")

    var i = 0

    while (i < 100) {
      var j = 0
      val start = System.nanoTime()

      Await.ready(conn.beginTx(), Duration.Inf)

      val insert = Await.result(conn.insert("insert into test(x) values (:x)"), Duration.Inf)

      while (j < 10000) {
        val rsFut = for {
          parametrized <- insert.bindF("x" -> j)
          _ <- parametrized.execute()
        } yield ()

        Await.ready(rsFut, Duration.Inf)
        j += 1
      }

      Await.ready(conn.commitTx(), Duration.Inf)

      val time = System.nanoTime() - start
      println(s"DONE: ${time / 1000000}ms")

      i += 1
    }

  }

}

object MultiConnTst extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder")

  val f = (1 to 10).map(_ => fact.connection()).reduce((f1, f2) => f1.flatMap(_ => f2))
  Await.ready(f, Duration.Inf)
  println("conns open")
}

object TypeTst extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder")

  fact
    .connection()
    .flatMap { conn =>
      println("hai\n\n\n")
      //Thread.sleep(20000L)
      val start = System.nanoTime()

      val x = -100

      val rsFut = for {
        stmt <- conn.statement(sql"SELECT x FROM interval_test")
        rs <- stmt.executeForSet()
      //TODO when i ctrl-c postgresql during pulling rows nothing happens
      } yield (stmt, rs)

      rsFut.flatMap {
        case (stmt, rs) =>
          rs.rows.foreach { row =>
            println("x = " + row.bytes("x"))
          }

          println("DONE")
          stmt.deallocate()
      }.map { _ =>
        conn.release()
      }

    }
    .recover {
      case ex =>
        println("ERROR")
        ex.printStackTrace()
    }
    .flatMap { _ =>
      println("hai shutdown")
      fact.shutdown()
    }
    .map { _ =>
      println("SHUT DOWN")
    }

}
object DataRowTst extends App {

  import scodec.bits._
  import scodec.codecs._

  val decoder = new ScodecDecoderFactory().decoder(Charset.forName("UTF-8"))


  var drs = new Array[ByteVector](100 * 10000)
  var i = 0
  var j = 0
  while (i < 100) {
    //println(i)
    while (j < 10000) {
      val x = int32.encode(j).require.bytes
      drs(i * 10000 + j) = (hex"440000000e000100000004" ++ x)
      j += 1
    }
    j = 0
    i += 1
  }

  println("no czesc")

  var x = 0
  while (x < 100) {
    val start = System.nanoTime()

    i = 0
    j = 0
    while (i < 100) {
      while (j < 10000) {
        decoder.decodeMsg(drs(i * 10000 + j))
        j += 1
      }
      j = 0
      i += 1
    }
    x += 1
    val time = System.nanoTime() - start
    println(s"$x: time = ${time / 1000000.0}")
  }


}


object ListenNotify extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = FiniteDuration.apply(10, "seconds")

  val fact = NettyPgConnectionFactory("localhost", 5432, "povder", "povder")

  fact
    .connection()
    .flatMap { conn =>
      println("hai\n\n\n")
      val start = System.nanoTime()

      val rsFut = for {
        stmt <- conn.statement(sql"LISTEN ch1")
        rs <- stmt.executeIgnoringResult()
      } yield (stmt, rs)

      rsFut.map { _ =>
        println("SLEEPING")
        Thread.sleep(10000L)
        conn.release()
      }

    }
    .recover {
      case ex =>
        println("ERROR")
        ex.printStackTrace()
    }
    .flatMap { _ =>
      println("hai shutdown")
      fact.shutdown()
    }
    .map { _ =>
      println("SHUT DOWN")
    }

}