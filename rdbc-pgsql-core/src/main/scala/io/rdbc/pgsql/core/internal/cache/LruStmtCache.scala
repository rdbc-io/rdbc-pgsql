package io.rdbc.pgsql.core.internal.cache

import io.rdbc.pgsql.core.internal.cache.LruStmtCache.StmtWithIdx
import io.rdbc.pgsql.core.pgstruct.messages.frontend.{NativeSql, StmtName}

import scala.collection.immutable.SortedMap

object LruStmtCache {

  private implicit val ord = Ordering.by[StmtWithIdx, Long](s => s.idx)

  private case class StmtWithIdx(stmtName: StmtName, idx: Long)

  def empty(capacity: Int): LruStmtCache = new LruStmtCache(capacity, 0L, Map.empty, SortedMap.empty)
}

class LruStmtCache private(val capacity: Int,
                           private val lastIdx: Long,
                           private val map: Map[NativeSql, StmtWithIdx],
                           private val ord: SortedMap[Long, NativeSql])
  extends StmtCache {

  import LruStmtCache._

  type Cache = LruStmtCache

  def get(sql: NativeSql): (LruStmtCache, Option[StmtName]) = {
    map.get(sql).map { case stmtWithIdx@StmtWithIdx(name, idx) =>
      val newIdx = nextIdx
      val newMap = map - sql + (sql -> stmtWithIdx.copy(idx = newIdx))
      val newOrd = ord - idx + (newIdx -> sql)
      val newCache = new LruStmtCache(capacity, newIdx, newMap, newOrd)
      (newCache, Some(name))
    }.getOrElse(this, None)
  }

  def put(sql: NativeSql, stmtName: StmtName): (LruStmtCache, Set[StmtName]) = {
    val newIdx = nextIdx
    val newMapElem = sql -> StmtWithIdx(stmtName, newIdx)
    val newOrdElem = newIdx -> sql
    if (capacity == map.size) {
      putAndEvictLru(newIdx, newMapElem, newOrdElem)
    } else {
      putNoEvict(newIdx, newMapElem, newOrdElem)
    }
  }

  private def putAndEvictLru(newIdx: Long,
                             newMapElem: (NativeSql, StmtWithIdx),
                             newOrdElem: (Long, NativeSql)): (LruStmtCache, Set[StmtName]) = {
    val (_, lruSql) = ord.head
    val evicted = map(lruSql).stmtName
    val newMap = map - lruSql + newMapElem
    val newOrd = ord.tail + newOrdElem
    val newCache = new LruStmtCache(capacity, newIdx, newMap, newOrd)
    (newCache, Set(evicted))
  }

  private def putNoEvict(newIdx: Long,
                         newMapElem: (NativeSql, StmtWithIdx),
                         newOrdElem: (Long, NativeSql)): (LruStmtCache, Set[StmtName]) = {
    val newMap = map + newMapElem
    val newOrd = ord + newOrdElem
    val newCache = new LruStmtCache(capacity, newIdx, newMap, newOrd)
    (newCache, Set.empty)
  }

  def evict(sql: NativeSql): Option[(LruStmtCache, StmtName)] = {
    map.get(sql).map { case StmtWithIdx(name, idx) =>
      val newMap = map - sql
      val newOrd = ord - idx
      val newCache = new LruStmtCache(capacity, lastIdx, newMap, newOrd)
      (newCache, name)
    }
  }

  private def nextIdx = lastIdx + 1

  override lazy val toString = {
    ord.map { case (_, sql) =>
      sql.value -> map(sql).stmtName.value
    }.mkString("LruStmtCache(", ", ", ")")
  }
}
