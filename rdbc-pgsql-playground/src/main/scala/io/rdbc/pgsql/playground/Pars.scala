package io.rdbc.pgsql.playground

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex


object Pars extends App {

  val doubleColonParam = """::[a-zA-Z]\w*"""
  val string = "'.+?'"
  val column = """".+?""""
  val blockComment = """/\*.*?\*/"""
  val lineComment = "--.*"
  val param = """(:[a-zA-Z]\w*)"""

  val pattern = new Regex(
    s"$string|$column|$blockComment|$lineComment|$doubleColonParam|$param", "param"
  )

  val sql = "'1'::int4 dupa :::dupa \":column\" :penor ':pstring' /*:p1*/ dupa :p2>:p3"

  val sb = new StringBuilder
  val params = ListBuffer.empty[String]
  var lastTextIdx = 0
  var lastParamIdx = 0

  pattern.findAllMatchIn(sql).foreach(println)

  pattern.findAllMatchIn(sql).filter(_.group(1) != null).foreach { m =>
    sb.append(sql.substring(lastTextIdx, m.start))
    lastParamIdx += 1
    sb.append("$").append(lastParamIdx)
    lastTextIdx = m.end
  }
  sb.append(sql.substring(lastTextIdx))

  println(sql)
  println(sb.toString())


}
