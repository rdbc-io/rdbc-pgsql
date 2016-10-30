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

package io.rdbc.pgsql.core

import java.nio.charset.Charset

object PgCharset {

  def toJavaCharset(pgCharset: String): Option[Charset] = mapping.get(pgCharset).map { javaCharsetName =>
    Charset.forName(javaCharsetName)
  }

  /**
    * Mapping between PostgreSQL charset name and IANA charset name used by java.nio.Charset
    */
  val mapping = Map(
    "BIG5" -> "Big5",
    "EUC_CN" -> "EUC_CN",
    "EUC_JP" -> "EUC-JP",
    "EUC_JIS_2004" -> "x-SJIS_0213", //TODO is this the same as SHIFT_JIS_2004?
    "EUC_KR" -> "EUC-KR",
    "EUC_TW" -> "x-EUC-TW",
    "GB18030" -> "GB18030",
    "GBK" -> "GBK",
    "ISO_8859_5" -> "ISO-8859-5",
    "ISO_8859_6" -> "ISO-8859-6",
    "ISO_8859_7" -> "ISO-8859-7",
    "ISO_8859_8" -> "ISO-8859-8",
    "JOHAB" -> "x-Johab",
    "KOI8R" -> "KOI8-R",
    "KOI8U" -> "KOI8-U",
    "LATIN1" -> "ISO-8859-1",
    "LATIN2" -> "ISO-8859-2",
    "LATIN3" -> "ISO-8859-3",
    "LATIN4" -> "ISO-8859-4",
    "LATIN5" -> "ISO-8859-5",
    "LATIN6" -> "ISO-8859-6",
    "LATIN7" -> "ISO-8859-7",
    "LATIN8" -> "ISO-8859-8",
    "LATIN9" -> "ISO-8859-9",
    "LATIN10" -> "ISO-8859-10",
    "SJIS" -> "Shift_JIS",
    "SHIFT_JIS_2004" -> "", //TODO
    "SQL_ASCII" -> "US-ASCII",
    "UHC" -> "MS949",
    "UTF8" -> "UTF-8",
    "WIN866" -> "IBM866", //TODO verify
    "WIN874" -> "x-windows-874",
    "WIN1250" -> "windows-1250",
    "WIN1251" -> "windows-1251",
    "WIN1252" -> "windows-1252",
    "WIN1253" -> "windows-1253",
    "WIN1254" -> "windows-1254",
    "WIN1255" -> "windows-1255",
    "WIN1256" -> "windows-1256",
    "WIN1257" -> "windows-1257",
    "WIN1258" -> "windows-1258"
  )
}
