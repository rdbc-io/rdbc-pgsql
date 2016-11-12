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

package io.rdbc.pgsql.core.util

import java.util.concurrent.locks.ReentrantLock

trait Lock {
  def withLock[A](block: => A): A
}

class SpinLock extends Lock {
  private[this] val rl = new ReentrantLock

  def withLock[A](block: => A): A = {
    while (rl.tryLock()) {}
    try {
      block
    } finally {
      rl.unlock()
    }
  }
}

class SleepLock extends Lock {
  private[this] val rl = new ReentrantLock

  def withLock[A](block: => A): A = {
    rl.lock()
    try {
      block
    } finally {
      rl.unlock()
    }
  }
}