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

package io.rdbc.pgsql.netty

import io.rdbc.pgsql.core.messages.backend.{ErrorMessage, StatusMessage}

object StatusMsgUtil {

  def isFatal(err: ErrorMessage): Boolean = {
    if (err.statusData.sqlState == "57014") {
      false //query canceled
    } else {
      val errCat = err.statusData.sqlState.take(2)
      errCat match {
        case "57" => true //operator intervention
        case "58" => true //system error
        case "XX" => true //PG internal error
        case _ => false
      }
    }
  }

  def isWarning(statusMsg: StatusMessage): Boolean = {
    val sqlState = statusMsg.statusData.sqlState
    sqlState.startsWith("01") || sqlState.startsWith("02")
  }

}
