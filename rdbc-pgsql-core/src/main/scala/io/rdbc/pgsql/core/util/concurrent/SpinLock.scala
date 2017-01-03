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

package io.rdbc.pgsql.core.util.concurrent

import java.util.concurrent.locks.ReentrantLock

import io.rdbc.util.Logging

class SpinLock extends Lock with Logging {
  private[this] val rl = new ReentrantLock

  def withLock[A](body: => A): A = {
    while (rl.tryLock()) {}
    try {
      body
    } finally {
      rl.unlock()
    }
  }
}

class SpinLockFactory extends LockFactory {
  def lock: SpinLock = new SpinLock
}
