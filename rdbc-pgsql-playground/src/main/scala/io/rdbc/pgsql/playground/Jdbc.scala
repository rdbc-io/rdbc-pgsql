package io.rdbc.pgsql.playground

import java.sql.DriverManager
import java.util.Properties

object Jdbc extends App {

  val props = new Properties()
  props.setProperty("user", "povder")
  props.setProperty("password", "povder")
  props.setProperty("binaryTransferEnable", "16423")
  val conn = DriverManager.getConnection("jdbc:postgresql://localhost/povder", props)
  try {
    val stmt = conn.prepareStatement("select * from geom_table")

    val rs = stmt.executeQuery()
    while (rs.next()) {
      println(rs.getObject(1))
    }


  } finally {
    conn.close()
  }


}
