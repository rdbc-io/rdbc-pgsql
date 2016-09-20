package io.rdbc.pgsql.playground

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import io.rdbc.pgsql.PgConnectionFactory
import io.rdbc.sapi.ResultSet
import org.reactivestreams.Publisher

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object SelectTest extends App {
  implicit val ec = ExecutionContext.global
  implicit val timeout = 10 seconds
  val fact = PgConnectionFactory("localhost", 5432, "povder", "povder", "povder", "povder")
  //TODO commit never happens during this test and a ClosePortal message is sent, which is unhandled
  fact.connection().flatMap { conn =>

    val idsFuture = for {
      stmt <- conn.select("select * from genkeytest where 1 = :one")
      boundStmt <- stmt.bindF("one" -> 1)
      id <- boundStmt.executeForValue[Int](_.int("id"))
    } yield id

    val fut = idsFuture.map { maybeId =>
      println("maybe id = " + maybeId)
    }

    conn.release()

    fut
  }.recover {
    case fatalEx => fatalEx.printStackTrace()
  }
}

object WarningTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = 10 seconds
  val fact = PgConnectionFactory("localhost", 5432, "povder", "povder", "povder", "povder")

  fact.connection().flatMap { conn =>

    val rsFuture = for {
      stmt <- conn.select("select pg_advisory_unlock(:one)")
      boundStmt <- stmt.bindF("one" -> 1)
      rs <- boundStmt.executeForSet()
    } yield rs

    rsFuture.map { rs =>
      println("warnings = " + rs.warnings)
    }

    conn.release()

  }.recover {
    case fatalEx => fatalEx.printStackTrace()
  }
}

object SelectMetadataTest extends App {

  implicit val ec = ExecutionContext.global
  implicit val timeout = 10 seconds
  val fact = PgConnectionFactory("localhost", 5432, "povder", "povder", "povder", "povder")

  fact.connection().flatMap { conn =>

    val metadataFut = for {
      stmt <- conn.select("select * from genkeytest where 1 = :one")
      boundStmt <- stmt.bindF("one" -> 1)
      rs <- boundStmt.executeForSet()
    } yield rs.metadata

    metadataFut.map { metadata =>
      println("metadata = " + metadata)
    }

    conn.release()

  }.recover {
    case fatalEx => fatalEx.printStackTrace()
  }
}

object InsertTest extends App {

  val fact = PgConnectionFactory("localhost", 5432, "povder", "povder", "povder", "povder")
  implicit val timeout = 10 seconds
  implicit val ec = ExecutionContext.global

  fact.connection().flatMap { conn =>

    val keyFuture = for {
      stmt <- conn.returningInsert("insert into genkeytest(name) values (:name)", "id")
      boundStmt <- stmt.bindF("name" -> "hai")
      key <- boundStmt.executeForLongKey()
    } yield key

    keyFuture.map { key =>
      println("generated key = " + key)
    }
  }.recover {
    case fatalEx => fatalEx.printStackTrace()
  }

}


object TxTest extends App {
  implicit val ec = ExecutionContext.global
  implicit val timeout = 10 seconds
  val fact = PgConnectionFactory("localhost", 5432, "povder", "povder", "povder", "povder")

  fact.connection().map { conn =>

    val nuthing = for {
      _ <- conn.beginTx()
      stmt <- conn.insert("insert into test(x) values (:one)")
      boundStmt <- stmt.bindF("one" -> 1)
      _ <- boundStmt.execute()
      _ <- conn.commitTx()
    } yield ()

    nuthing.map { _ =>
      println("COMITTED")
    }.recover {
      case ex =>
        ex.printStackTrace()
        conn.rollbackTx()
    }
    conn.release()
  }


  /*
    //TODO pgSession is not unique, fix this
    val x: XorF[DbOperationEx, Unit] = fact.connection().leftMap(DbOperationEx.Connect).flatMap { conn =>
      conn.beginTx().leftMap(DbOperationEx.Begin).flatMap { _ =>
        conn.insert("insert into test(x) values (:one)").bindF("one" -> 1).leftMap(DbOperationEx.Bind).flatMap { stmt =>
          stmt.execute().leftMap(DbOperationEx.Execute).flatMap { _ =>
            conn.commitTx().leftMap(DbOperationEx.Commit)
          }
        }
      }
    } */

}

object ParseTest extends App {
  implicit val ec = ExecutionContext.global
  val fact = PgConnectionFactory("localhost", 5432, "povder", "povder", "povder", "povder")
  implicit val timeout = 10 seconds
  //TODO this test doesn't work anymore, closePportal is sent and that's it
  val futResult: Future[Unit] = fact.connection().flatMap { conn =>

    val rsFut = for {
      stmt <- conn.statement("select * from test where x = :one")
      boundStmt <- stmt.bindF("one" -> 1)
      rs <- boundStmt.executeForSet()

      stmt2 <- conn.statement("select * from test where x = :one")
      boundStmt2 <- stmt2.bindF("one" -> 1)
      rs2 <- boundStmt2.executeIgnoringResult()

      stmt3 <- conn.statement("select * from test where x = :one")
      boundStmt3 <- stmt3.bindF("one" -> 1)
      rs3 <- boundStmt3.executeForSet()
    } yield rs

    val traverseResult = rsFut.map { rs =>
      rs.rows.zipWithIndex.foreach { case (row, idx) =>
        val x = row.int("x")
        val t = row.localDateTime("t")
        println(s"$idx: x=$x, t=$t")
      }
    }

    conn.release().recover { case releaseErr => println("WARN: failed to release the connection " + releaseErr) }

    traverseResult
  }

  futResult.onComplete {
    case Failure(ex) =>
      println("FATAL error, future did not complete successfully")
      ex.printStackTrace()

    case Success(_) => println("success")
  }

}

object SleepTest extends App {
  implicit val ec = ExecutionContext.global
  val fact = PgConnectionFactory("localhost", 5432, "povder", "povder", "povder", "povder")
  implicit val timeout = 5 seconds

  val futResult: Future[ResultSet] = fact.connection().flatMap { conn =>

    val rsFut = for {
      stmt <- conn.statement("select pg_sleep(:duration)")
      boundStmt <- stmt.bindF("duration" -> 3)
      rs <- boundStmt.executeForSet()
    } yield rs


    conn.release().recover { case releaseErr => println("WARN: failed to release the connection " + releaseErr) }

    rsFut
  }

  futResult.onComplete {
    case Failure(ex) =>
      println("FATAL error, future did not complete successfully")
      ex.printStackTrace()

    case Success(_) => println("success")
  }

}


object StreamInsertsTest extends App {
  implicit val ec = ExecutionContext.global
  val fact = PgConnectionFactory("localhost", 5432, "povder", "povder", "povder", "povder")
  implicit val timeout = 5 seconds

  implicit val implActorSystem = ActorSystem("Dsads")
  implicit val materializer = ActorMaterializer()

  val params = (1 to 20).map(i => Map[String, Any]("one" -> i))

  val src: Publisher[Map[String, Any]] = Source(params).runWith(Sink.asPublisher(fanout = false))

  val futResult: Future[Unit] = fact.connection().flatMap { conn =>

    val nuthing = for {
      stmt <- conn.statement("insert into test(x) values (:one)")
      _ <- stmt.streamParams(src)
    } yield ()


    conn.release().recover { case releaseErr => println("WARN: failed to release the connection " + releaseErr) }

    nuthing
  }

  futResult.onComplete {
    case Failure(ex) =>
      println("FATAL error, future did not complete successfully")
      ex.printStackTrace()

    case Success(_) => println("success")
  }

}