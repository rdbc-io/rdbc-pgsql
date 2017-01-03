package io.rdbc.pgsql.core.internal.cache

import io.rdbc.pgsql.core.pgstruct.messages.frontend.{NativeSql, StmtName}

trait StmtCache {
  type Cache <: StmtCache
  def get(sql: NativeSql): (Cache, Option[StmtName])
  def put(sql: NativeSql, stmtName: StmtName): (Cache, Set[StmtName])
  def evict(sql: NativeSql): Option[(Cache, StmtName)]
}
