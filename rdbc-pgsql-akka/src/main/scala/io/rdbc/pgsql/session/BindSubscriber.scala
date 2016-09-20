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

import akka.actor.Props
import akka.stream.actor.{ActorSubscriber, MaxInFlightRequestStrategy}
import io.rdbc.pgsql.core.messages.frontend.Bind

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

object BindSubscriber {
  def props: Props = Props(new BindSubscriber)
}

class BindSubscriber extends ActorSubscriber {

  import akka.stream.actor.ActorSubscriberMessage._

  implicit val ec: ExecutionContext = context.dispatcher

  val MaxQueueSize = 10
  //TODO make it configurable
  private var queue = Vector.empty[Bind] //TODO decide about queue impl

  override val requestStrategy = new MaxInFlightRequestStrategy(max = MaxQueueSize) {
    override def inFlightInternally: Int = queue.size
  } //TODO

  def receive = {
    case OnNext(bind: Bind) =>
      println("NEXT BIND KURWA: " + bind)
      queue :+= bind
      akka.pattern.after(3 seconds, using = context.system.scheduler) {
        self ! "ok"
        Future.successful(())
      }

    case "ok" =>
      println(s"BIND processed")
      queue = queue.drop(1)

    case OnError(ex) =>
      println("ON ERROR KURWA " + ex)
      ()

    case OnComplete =>
      println("ON COMPLETE KURWA")
      ()
  }
}
