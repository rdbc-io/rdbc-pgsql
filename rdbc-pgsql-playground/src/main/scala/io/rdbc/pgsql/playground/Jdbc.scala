package io.rdbc.pgsql.playground

import java.sql.DriverManager
import java.util.Properties

object Jdbc extends App {

  val props = new Properties()
  props.setProperty("user", "povder")
  props.setProperty("password", "povder")
  val conn = DriverManager.getConnection("jdbc:postgresql://localhost/povder", props)
  try {
    val stmt = conn.prepareStatement("insert into test(x) values (?)")
    (1 to 100).foreach { i =>
      stmt.setInt(1, i)
      stmt.addBatch()
    }
    stmt.executeBatch()

  } finally {
    conn.close()
  }


}
