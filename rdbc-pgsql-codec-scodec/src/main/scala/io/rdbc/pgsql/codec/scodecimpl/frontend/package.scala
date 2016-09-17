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

package io.rdbc.pgsql.codec.scodecimpl.msg

import java.nio.charset.Charset

import io.rdbc.pgsql.codec.scodecimpl.pg._
import io.rdbc.pgsql.codec.scodecimpl.suppl._
import io.rdbc.pgsql.core.messages.frontend._
import scodec.Codec
import scodec.codecs._

package object frontend {

  def bind(implicit charset: Charset): Codec[Bind] = pgHeadedMsg('B') {
    {
      ("portal" | pgStringOption) ::
        ("statement" | pgStringOption) ::
        ("paramValues" | new ParamValuesCodec) ::
        ("resultColumns" | new ReturnFieldFormatsCodec)
    }.as[Bind]
  }

  def describe(implicit charset: Charset): Codec[Describe] = pgHeadedMsg('D') {
    {
      ("describeType" | {
        discriminated[DescribeType].by(byte)
          .typecase('S', provide(PreparedStatementType))
          .typecase('P', provide(PortalType))
      }) :: ("optionalName" | pgStringOption)
    }.as[Describe]
  }

  def execute(implicit charset: Charset): Codec[Execute] = pgHeadedMsg('E') {
    {
      ("portalName" | pgStringOption) ::
        ("fetchSize" | pgInt32Option)
    }.as[Execute]
  }

  val flush: Codec[Flush.type] = pgSingletonHeadedMsg('F', Flush)

  def parse(implicit charset: Charset): Codec[Parse] = pgHeadedMsg('P') {
    {
      ("preparedStmt" | pgStringOption) ::
        ("query" | pgString) ::
        ("paramTypes" | listOfN(pgInt16, oid))
    }.as[Parse]
  }

  val password: Codec[PasswordMessage] = pgHeadedMsg('p') {
    {
      "credentials" | bytes
    }.as[PasswordMessage]
  }.withToString("PasswordMessage")

  def query(implicit charset: Charset): Codec[Query] = pgHeadedMsg('Q') {
    ("query" | pgString).as[Query]
  }

  def startup(implicit charset: Charset): Codec[StartupMessage] = {
    val ver3_0 = pgInt32.unit(196608)

    pgHeadlessMsg(
      {
        ("protocol version" | ver3_0) ~>
          pgString.unit("user") ~> ("user" | pgString) ::
          pgString.unit("database") ~> ("database" | pgString) ::
          ("options" | pgParamMap(pgString))
      }.as[StartupMessage]
    ).withToString("StartupMessage")
  }

  val terminate: Codec[Terminate.type] = pgSingletonHeadedMsg('X', Terminate)

  val sync: Codec[Sync.type] = pgSingletonHeadedMsg('S', Sync)

  def cancelRequest: Codec[CancelRequest] = {
    pgHeadlessMsg(
      {
        ("cancel code" | pgInt32.unit(80877102)) ~>
          ("process ID" | pgInt32) ::
          ("secret key" | pgInt32)
      }.as[CancelRequest]
    ).withToString("CancelRequest")
  }

  def closeStatement(implicit charset: Charset): Codec[CloseStatement] = pgHeadedMsg('C') {
    {
      byte.unit('S') ~> ("optionalName" | pgStringOption)
    }.as[CloseStatement]
  }

  def closePortal(implicit charset: Charset): Codec[ClosePortal] = pgHeadedMsg('C') {
    {
      byte.unit('P') ~> ("optionalName" | pgStringOption)
    }.as[ClosePortal]
  }

}
