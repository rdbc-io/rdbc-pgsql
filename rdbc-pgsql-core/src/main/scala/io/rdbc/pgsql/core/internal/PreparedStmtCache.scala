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

package io.rdbc.pgsql.core.internal

import io.rdbc.pgsql.core.pgstruct.messages.frontend.{NativeSql, StmtName}

private[core] object PreparedStmtCache {
  val empty = new PreparedStmtCache(Map.empty)
}

private[core] class PreparedStmtCache(cache: Map[NativeSql, StmtName]) {
  //TODO should I cache using NativeSql or RdbcSql?

  //TODO replace with LRU or sth
  //TODO when element is evicted from the cache CloseStatement needs to be sent to the backend

  def get(sql: NativeSql): Option[StmtName] = cache.get(sql)

  def updated(sql: NativeSql, stmtName: StmtName): PreparedStmtCache = {
    new PreparedStmtCache(cache + (sql -> stmtName))
  }
}
