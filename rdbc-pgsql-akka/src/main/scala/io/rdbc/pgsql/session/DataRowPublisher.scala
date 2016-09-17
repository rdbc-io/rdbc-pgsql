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

package io.rdbc.pgsql.session

import java.util.concurrent.TimeoutException

import akka.actor.Status.{Failure, Success}
import akka.actor.{ActorRef, Props}
import akka.stream.actor.ActorPublisher
import io.rdbc.pgsql.core.messages.backend.DataRow
import io.rdbc.pgsql.core.messages.frontend.{ClosePortal, Execute, Sync}
import io.rdbc.pgsql.transport.ConnectionManager.Write

import scala.concurrent.duration.{Duration, SECONDS}

object DataRowPublisher {
  case object Resume
  def props(conn: ActorRef, portal: Option[String]) = Props(classOf[DataRowPublisher], conn, portal)
}

class DataRowPublisher(conn: ActorRef, portalName: Option[String]) extends ActorPublisher[DataRow] {

  import akka.stream.actor.ActorPublisherMessage._
  import DataRowPublisher._

  private var canQueryMore = true

  def receive = {
    case row: DataRow =>
      onNext(row)

    case Resume =>
      canQueryMore = true
      queryDb()

    case Request(n) =>
      if (canQueryMore) {
        queryDb()
      }

    case Failure(cause) =>
      onError(cause)
      context.stop(self)

    case Success(_) =>
      onCompleteThenStop()

    case SubscriptionTimeoutExceeded =>
      onError(new TimeoutException()) //TODO better cause
      conn ! Write(ClosePortal(portalName), Sync) //TODO test this
      context.stop(self)

    case Cancel =>
      conn ! Write(ClosePortal(portalName), Sync) //TODO test this
      onCompleteThenStop()
  }

  private def queryDb() = {
    if (totalDemand != 0L) {
      /*
        toInt truncation is safe here, on next Resume remaining demand will be pulled from the DB
        in an unlikely event of demand exceeding Int.MaxValue
       */
      val execute = Execute(portalName, Some(totalDemand.toInt))
      conn ! Write(execute, Sync)
      canQueryMore = false
    }
  }

  override val subscriptionTimeout = Duration(21474835, SECONDS) //TODO make it configurable

}
