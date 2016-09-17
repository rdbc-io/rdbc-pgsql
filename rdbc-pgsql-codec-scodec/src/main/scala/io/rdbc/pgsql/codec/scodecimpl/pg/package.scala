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

package io.rdbc.pgsql.codec.scodecimpl

import java.nio.charset.Charset

import scodec.Codec
import scodec.bits.BitVector
import scodec.codecs._
import suppl._

package object pg {

  def pgString(implicit charset: Charset) = nulTerminated(string(charset)).withToString("pgString")

  def pgStringNonTerminated(implicit charset: Charset) = string(charset).withToString("pgStringNonTerminated")

  implicit val pgInt32 = int32.withToString("pgInt32")
  implicit val pgInt16 = int16.withToString("pgInt16")

  implicit def pgStringOption(implicit charset: Charset) = pgString.xmap[Option[String]](s => {
    if (s.isEmpty) None
    else Some(s)
  }, {
    case Some(s) => s
    case None => ""
  })

  implicit val pgInt16Option = pgInt16.xmap[Option[Int]](i => {
    if (i == 0) None
    else Some(i)
  }, {
    case Some(i) => i
    case None => 0
  })

  implicit val pgInt32Option = pgInt32.xmap[Option[Int]](i => {
    if (i == 0) None
    else Some(i)
  }, {
    case Some(i) => i
    case None => 0
  })

  def pgParam[K](keyCodec: Codec[K])(implicit charset: Charset): Codec[(K, String)] = {
    ("key" | keyCodec) ~
      ("value" | pgString)
  }.withToString("pgParam")

  def pgParamMap[K](keyCodec: Codec[K])(implicit charset: Charset): Codec[Map[K, String]] = new PgMapCodec[K](pgParam(keyCodec))
}


