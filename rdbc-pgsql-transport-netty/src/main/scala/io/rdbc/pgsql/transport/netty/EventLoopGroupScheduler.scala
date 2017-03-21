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

package io.rdbc.pgsql.transport.netty

import io.netty.channel.EventLoopGroup
import io.rdbc.pgsql.core.internal.scheduler.{ScheduledTask, TaskScheduler}
import io.rdbc.util.Logging

import scala.concurrent.duration.FiniteDuration

private[netty] class EventLoopGroupScheduler(eventLoopGroup: EventLoopGroup)
  extends TaskScheduler
    with Logging {

  def schedule(delay: FiniteDuration)(action: => Unit): ScheduledTask = traced {
    logger.debug(s"Scheduling a task to run in $delay using ELG $eventLoopGroup")
    val fut = eventLoopGroup.schedule(runnable(action), delay.length, delay.unit)
    new NettyScheduledTask(fut)
  }

  /* Scala 2.11 compat */
  private def runnable(action: => Unit): Runnable = {
    new Runnable() {
      def run(): Unit = action
    }
  }
}
