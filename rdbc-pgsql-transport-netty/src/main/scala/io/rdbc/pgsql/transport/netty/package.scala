/*
 * Copyright 2016-2017 Krzysztof Pado
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

package io.rdbc.pgsql.transport

import io.netty.channel.{Channel, ChannelFuture}
import io.netty.util.concurrent.{GenericFutureListener, Future => NettyFuture}

import scala.concurrent.{ExecutionContext, Future, Promise}

package object netty {

  private[netty] implicit class NettyFut2Scala[T](nettyFut: NettyFuture[T]) {

    def scalaFut: Future[T] = {
      val promise = Promise[T]
      nettyFut.addListener(genericFutureListener { future =>
        if (future.isSuccess) promise.success(future.get())
        else promise.failure(future.cause())
      })
      promise.future
    }

    /* Scala 2.11 compat */
    private def genericFutureListener(callback: NettyFuture[T] => Unit): GenericFutureListener[NettyFuture[T]] = {
      new GenericFutureListener[NettyFuture[T]] {
        def operationComplete(future: NettyFuture[T]): Unit = {
          callback(future)
        }
      }
    }
  }

  private[netty] implicit class NettyVoidFut2Scala(nettyFut: NettyFuture[Void]) {
    def scalaFut(implicit ec: ExecutionContext): Future[Unit] = {
      new NettyFut2Scala(nettyFut).scalaFut.map(_ => ())
    }
  }

  private[netty] implicit class NettyChannelFut2Scala(channelFut: ChannelFuture) {
    def scalaFut(implicit ec: ExecutionContext): Future[Channel] = {
      new NettyFut2Scala(channelFut).scalaFut.map(_ => channelFut.channel())
    }
  }
}
