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

package io.rdbc.pgsql.scodec.msg

import java.nio.charset.Charset

import _root_.scodec.codecs._
import _root_.scodec.{Attempt, Codec, Err, SizeBound}
import io.rdbc.pgsql.core.pgstruct.messages.backend.{PgKey, PgPid}
import io.rdbc.pgsql.core.pgstruct.messages.frontend._
import io.rdbc.pgsql.scodec.ParamValuesCodec.paramValues
import io.rdbc.pgsql.scodec._
import scodec.bits.BitVector

package object frontend {

  private[scodec] def pgFrontendMessage(implicit charset: Charset): scodec.Encoder[PgFrontendMessage] = {
    new scodec.Encoder[PgFrontendMessage] {
      val sizeBound: SizeBound = SizeBound.unknown

      def encode(msg: PgFrontendMessage): Attempt[BitVector] = {
        val codec = msg match {
          case _: Startup           => startup.upcast[PgFrontendMessage]
          case _: Bind              => bind.upcast[PgFrontendMessage]
          case _: DescribePortal    => describePortal.upcast[PgFrontendMessage]
          case _: DescribeStatement => describeStatement.upcast[PgFrontendMessage]
          case _: Execute           => execute.upcast[PgFrontendMessage]
          case _: Parse             => parse.upcast[PgFrontendMessage]
          case _: PasswordMessage   => password.upcast[PgFrontendMessage]
          case _: Query             => query.upcast[PgFrontendMessage]
          case _: CancelRequest     => cancelRequest.upcast[PgFrontendMessage]
          case _: CloseStatement    => closeStatement.upcast[PgFrontendMessage]
          case _: ClosePortal       => closePortal.upcast[PgFrontendMessage]
          case Terminate            => terminate.upcast[PgFrontendMessage]
          case Flush                => flush.upcast[PgFrontendMessage]
          case Sync                 => sync.upcast[PgFrontendMessage]
          case _                    => fail[PgFrontendMessage] {
                                         Err(s"Encoding message of type ${msg.getClass} is not supported")
                                       }
        }
        codec.encode(msg)
      }
    }
  }

  private def portalName(implicit charset: Charset): Codec[PortalName] = stringNul.as[PortalName]

  private def maybePortalName(implicit charset: Charset): Codec[Option[PortalName]] = {
    maybe(portalName, PortalName(""))
  }

  private def stmtName(implicit charset: Charset): Codec[StmtName] = stringNul.as[StmtName]

  private def maybeStmtName(implicit charset: Charset): Codec[Option[StmtName]] = {
    maybe(stmtName, StmtName(""))
  }

  private def nativeSql(implicit charset: Charset): Codec[NativeSql] = stringNul.as[NativeSql]

  private def bind(implicit charset: Charset): Codec[Bind] = {
    pgHeadedMsg('B') {
      {
        ("portal"         | maybePortalName) ::
        ("statement"      | maybeStmtName) ::
        ("param_values"   | paramValues) ::
        ("result_columns" | ReturnFieldFormatsCodec)
      }.as[Bind]
    }
  }

  private def describeStatement(implicit charset: Charset): Codec[DescribeStatement] = pgHeadedMsg('D') {
    {
      ("describeType" | byte.unit('S')) ~> ("optionalName" | maybeStmtName)
    }.as[DescribeStatement]
  }

  private def describePortal(implicit charset: Charset): Codec[DescribePortal] = pgHeadedMsg('D') {
    {
      ("describeType" | byte.unit('P')) ~> ("optionalName" | maybePortalName)
    }.as[DescribePortal]
  }

  private def execute(implicit charset: Charset): Codec[Execute] = pgHeadedMsg('E') {
    {
      ("portalName" | maybePortalName) ::
      ("fetchSize"  | maybeInt32)
    }.as[Execute]
  }

  private val flush: Codec[Flush.type] = pgSingletonHeadedMsg('F', Flush)

  private def parse(implicit charset: Charset): Codec[Parse] = pgHeadedMsg('P') {
    {
      ("preparedStmt" | maybeStmtName) ::
      ("query"        | nativeSql) ::
      ("paramTypes"   | vectorOfN(int16, oid))
    }.as[Parse]
  }

  private val password: Codec[PasswordMessage] = pgHeadedMsg('p') {
    ("credentials" | bytes).as[PasswordMessage]
  }

  private def query(implicit charset: Charset): Codec[Query] = pgHeadedMsg('Q') {
    ("query" | nativeSql).as[Query]
  }

  private def startup(implicit charset: Charset): Codec[Startup] = {
    val ver3_0 = int32.unit(196608)

    pgHeadlessMsg(
      {
        ("protocol version" | ver3_0) ~>
        ("user key"         | stringNul.unit("user")) ~>
        ("user"             | stringNul) ::
        ("db key"           | stringNul.unit("database")) ~>
        ("database"         | stringNul) ::
        ("options"          | pgParamMap(stringNul))
      }.as[Startup]
    ).withToString("StartupMessage")
  }

  private val terminate: Codec[Terminate.type] = pgSingletonHeadedMsg('X', Terminate)

  private val sync: Codec[Sync.type] = pgSingletonHeadedMsg('S', Sync)

  private def cancelRequest: Codec[CancelRequest] = {
    pgHeadlessMsg(
      {
        ("cancel code" | int32.unit(80877102)) ~>
        ("process ID"  | int32).as[PgPid] ::
        ("secret key"  | int32).as[PgKey]
      }.as[CancelRequest]
    )
  }

  private def closeStatement(implicit charset: Charset): Codec[CloseStatement] = pgHeadedMsg('C') {
    {
      byte.unit('S') ~> ("optionalName" | maybeStmtName)
    }.as[CloseStatement]
  }

  private def closePortal(implicit charset: Charset): Codec[ClosePortal] = pgHeadedMsg('C') {
    {
      byte.unit('P') ~> ("optionalName" | maybePortalName)
    }.as[ClosePortal]
  }

}
