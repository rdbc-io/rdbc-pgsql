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

package io.rdbc.pgsql.core.internal.fsm

private[core] trait NonFatalErrorsAreFatal {
  this: State =>

  protected def onError(ex: Throwable): Unit

  protected final def onFatalError(ex: Throwable): Unit = traced {
    onError(ex)
  }

  protected final def onNonFatalError(ex: Throwable): StateAction = {
    logger.debug(s"State '$this' does not override non-fatal error handler, treating error as fatal")
    fatal(ex) andThen onFatalErrorF(ex)
  }
}
