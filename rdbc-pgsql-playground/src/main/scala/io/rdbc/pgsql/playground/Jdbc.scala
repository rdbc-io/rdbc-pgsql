package io.rdbc.pgsql.playground

import java.sql.DriverManager

object Jdbc extends App {

  val conn = DriverManager.getConnection("jdbc:postgresql://localhost/povder", "povder", "povder")
  try {
    val stmt = conn.prepareStatement("insert into bool_test (c) values (?)")
    stmt.setString(1, "N")
    stmt.executeUpdate()

    val select = conn.createStatement()
    val rs = select.executeQuery("select c from bool_test")
    while(rs.next()) {
      println(rs.getString(1) + " " +rs.getBoolean(1))
    }
  } finally {
    conn.close()
  }


}
