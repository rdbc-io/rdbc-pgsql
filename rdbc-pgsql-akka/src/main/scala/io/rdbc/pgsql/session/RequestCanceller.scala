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

import java.net.InetSocketAddress

import akka.actor.{Actor, Props}
import io.rdbc.pgsql.codec.{Decoder, Encoder}
import io.rdbc.pgsql.core.messages.frontend.CancelRequest
import io.rdbc.pgsql.transport.ConnectionManager.{Close, Connect, Connected, Write}
import io.rdbc.pgsql.transport.akkaio.AkkaIoConnectionManager

object RequestCanceller {
  def props(address: InetSocketAddress, encoder: Encoder, decoder: Decoder) = Props(classOf[RequestCanceller], address, encoder, decoder)
}

class RequestCanceller(address: InetSocketAddress, encoder: Encoder, decoder: Decoder) extends Actor {

  private val conn = context.actorOf(AkkaIoConnectionManager.props(self, encoder, decoder), name = "cancellingConnection")

  def receive = {
    case req: CancelRequest =>
      conn ! Connect(address)

      context become {
        case Connected =>
          println("writing cancel req")
          conn ! Write(req)
          conn ! Close
          context.stop(self)

//TODO conn failed etc
        case any =>
          println("ANY " + any)
      }

    case any => println("KURWA")
  }
}
