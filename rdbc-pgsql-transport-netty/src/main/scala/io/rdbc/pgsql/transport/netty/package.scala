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

package io.rdbc.pgsql.transport

import io.netty.util.concurrent.{GenericFutureListener, Future => NettyFuture}

import scala.concurrent.{ExecutionContext, Future, Promise}

package object netty {

  implicit class NettyFut2Scala[T](nettyFut: NettyFuture[T]) {

    def scalaFut: Future[T] = {
      val promise = Promise[T]
      nettyFut.addListener(new GenericFutureListener[NettyFuture[T]] {
        def operationComplete(future: NettyFuture[T]) = {
          if (future.isSuccess) promise.success(future.get())
          else promise.failure(future.cause())
        }
      })
      promise.future
    }
  }

  implicit class NettyVoidFut2Scala(nettyFut: NettyFuture[Void]) {
    def scalaFut(implicit ec: ExecutionContext): Future[Unit] = {
      new NettyFut2Scala(nettyFut).scalaFut.map(_ => ())
    }
  }
}
