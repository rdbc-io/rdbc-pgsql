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

package io.rdbc.pgsql.core.internal

import io.rdbc.util.Logging
import io.rdbc.util.scheduler.{ScheduledTask, TaskScheduler}

import scala.concurrent.duration.FiniteDuration

private[core] class TimeoutHandler(scheduler: TaskScheduler,
                                   timeout: FiniteDuration,
                                   timeoutAction: () => Unit)
  extends Logging {

  def scheduleTimeoutTask(reqId: RequestId): ScheduledTask = traced {
    logger.debug(s"Scheduling a timeout task for request '$reqId' to run in $timeout using scheduler '$scheduler'")
    scheduler.schedule(timeout)(timeoutAction)
  }
}
