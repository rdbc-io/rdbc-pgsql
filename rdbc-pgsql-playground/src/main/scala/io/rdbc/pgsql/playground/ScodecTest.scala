package io.rdbc.pgsql.playground

import java.nio.charset.Charset

import io.rdbc.pgsql.core.messages.backend.{DataRow, PgBackendMessage, ServerCharset}
import io.rdbc.pgsql.core.messages.data.DbValFormat._
import io.rdbc.pgsql.core.messages.data.{DbValFormat, NotNullFieldValue}
import io.rdbc.pgsql.core.messages.frontend._
import scodec.Encoder


object ScodecTest extends App {
/*
  implicit val serverCharset = ServerCharset(Charset.forName("UTF-8"))
  implicit val clientCharset = ClientCharset(Charset.forName("UTF-8"))

  import scodec.bits._

  val binFormat = BinaryDbValFormat
  val binFormatBytes = DbValFormat.codec.encode(binFormat)
  println(s"BinFormatBytes = $binFormatBytes")


  val bind = Bind(Some("portalnam"), None, List(
    TextualParamValue("jeden"),
    TextualParamValue("dwa"),
    NullParamValue
  ), SpecificFieldFormats(List(BinaryDbValFormat, TextualDbValFormat)))

  val bindBytes = Encoder.encode(bind).require.bytes

  println(s"bind = $bindBytes")

  val data = PgBackendMessage.codec.encode(new DataRow(List(
    NotNullFieldValue(hex"0x123456789012345678901234567890123456789012345678901234567890"),
    NotNullFieldValue(hex"0x123456789012345678901234567890123456789012345678901234567890")
  ))).require

  PgBackendMessage.codec.decode(hex"0x440000004a00020000001e1234567890123456789012345678901234567890123456789012345678900000001e123456789012345678901234567890123456789012345678901234567890".bits)
*/
}
