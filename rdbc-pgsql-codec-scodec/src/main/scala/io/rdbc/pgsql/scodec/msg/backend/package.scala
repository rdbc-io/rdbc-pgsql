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
import _root_.scodec.{Attempt, Codec, DecodeResult, Err, SizeBound}
import io.rdbc.pgsql.core.pgstruct.TxStatus
import io.rdbc.pgsql.core.pgstruct.messages.backend._
import io.rdbc.pgsql.core.pgstruct.messages.backend.auth.{AuthBackendMessage, AuthOk, AuthRequestMd5}
import io.rdbc.pgsql.scodec._
import scodec.bits.BitVector

package object backend {

  private[scodec] def pgBackendMessage(implicit charset: Charset): Codec[PgBackendMessage] = {
    val msgTypeContext = "msg_type_id"

    def unknownMsgFallback(msgCodec: Codec[PgBackendMessage]) = new Codec[PgBackendMessage] {
      def sizeBound: SizeBound = unknown.sizeBound | msgCodec.sizeBound
      def encode(msg: PgBackendMessage): Attempt[BitVector] = {
        msg match {
          case ubm: UnknownBackendMessage => unknown.encode(ubm)
          case _ => msgCodec.encode(msg)
        }
      }

      def decode(b: BitVector): Attempt[DecodeResult[PgBackendMessage]] = {
        msgCodec.decode(b).recoverWith {
          case err: KnownDiscriminatorType[_]#UnknownDiscriminator
            if err.context.last == msgTypeContext =>
            unknown.decode(b)
        }
      }
    }

    unknownMsgFallback(
       discriminated[PgBackendMessage]
        .by(msgTypeContext | byte)
        .typecase('S', "param_status_msg"         | parameterStatus)
        .typecase('R', "auth_msg"                 | auth)
        .typecase('Z', "ready_for_query_msg"      | readyForQuery)
        .typecase('T', "row_desc_msg"             | rowDescription)
        .typecase('D', "data_row_msg"             | dataRow)
        .typecase('C', "command_complete_msg"     | commandComplete)
        .typecase('K', "backend_key_data_msg"     | backendKeyData)
        .typecase('1', "parse_complete_msg"       | parseComplete)
        .typecase('2', "bind_complete_msg"        | bindComplete)
        .typecase('3', "close_complete_msg"       | closeComplete)
        .typecase('I', "empty_query_response_msg" | emptyQueryResponse)
        .typecase('E', "error_msg"                | error)
        .typecase('N', "notice_msg"               | notice)
        .typecase('s', "portal_suspended_msg"     | portalSuspended)
        .typecase('n', "no_data_msg"              | noData)
        .typecase('t', "param_desc_msg"           | parameterDescription)
        .typecase('A', "notification_respo_msg"   | notificationResponse)
    )
  }

  private val noData: Codec[NoData.type] = {
    pgSingletonHeadlessMsg(NoData)
  }

  private val closeComplete: Codec[CloseComplete.type] = {
    pgSingletonHeadlessMsg(CloseComplete)
  }

  private val portalSuspended: Codec[PortalSuspended.type] = {
    pgSingletonHeadlessMsg(PortalSuspended)
  }

  private val parseComplete: Codec[ParseComplete.type] = {
    pgSingletonHeadlessMsg(ParseComplete)
  }

  private val bindComplete: Codec[BindComplete.type] = {
    pgSingletonHeadlessMsg(BindComplete)
  }

  private val emptyQueryResponse: Codec[EmptyQueryResponse.type] = {
    pgSingletonHeadlessMsg(EmptyQueryResponse)
  }

  private def commandComplete(implicit charset: Charset): Codec[CommandComplete] = pgHeadlessMsg {
    stringNul.xmap[CommandComplete](
      message => {
        if (CommandComplete.RowCountMessages.exists(rowCountMsg => message.startsWith(rowCountMsg))) {
          val (constant, rowsStr) = message.splitAt(message.lastIndexOf(" ") + 1) //TODO err handling
          CommandComplete(constant, Some(rowsStr.toInt))
        } else {
          CommandComplete(message, None)
        }
      }, {
        case CommandComplete(message, None) => message
        case CommandComplete(message, Some(rowCount)) => s"$message $rowCount"
      }
    )
  }

  private def parameterStatus(implicit charset: Charset): Codec[ParameterStatus] = pgHeadlessMsg {
    pgParam(stringNul).withContext("status_param").xmap(
      {
        case (key, value) => ParameterStatus(SessionParamKey(key), SessionParamVal(value))
      },
      p => (p.key.value, p.value.value)
    )
  }

  private def dataRow(implicit charset: Charset): Codec[DataRow] = pgHeadlessMsg {
    vectorOfN(
      "colCount"  | int16,
      "colValues" | colValue
    ).withContext("columns")
     .as[DataRow]
  }

  private val unknown: Codec[UnknownBackendMessage] = {
    {
      ("head" | byte) ::
      ("body" | variableSizeBytes(int32, bytes, 4))
    }.as[UnknownBackendMessage]
  }

  private val readyForQuery: Codec[ReadyForQuery] = pgHeadlessMsg {
    discriminated
      .by("tx_status_id" | byte)
      .typecase('I', provide(ReadyForQuery(TxStatus.Idle)))
      .typecase('E', provide(ReadyForQuery(TxStatus.Failed)))
      .typecase('T', provide(ReadyForQuery(TxStatus.Active)))
      .withContext("tx_status")
  }

  private val authRequestMd5: Codec[AuthRequestMd5] = {
    bytes(4).withContext("md5_salt").as[AuthRequestMd5]
  }

  private val auth: Codec[AuthBackendMessage] = pgHeadlessMsg {
    discriminated[AuthBackendMessage]
      .by("auth_type_id" | int32)
      .typecase(0x00, provide(AuthOk))
      .typecase(0x05, authRequestMd5)
      .withContext("auth_type")
  }

  private def rowDescription(implicit charset: Charset): Codec[RowDescription] = pgHeadlessMsg {
    vectorOfN(
      "col_count" | int16.withContext("col_count"),
      "col_desc"  | colDesc
    ).withContext("col_descs")
     .as[RowDescription]
  }

  private val backendKeyData: Codec[BackendKeyData] = pgHeadlessMsg {
    {
      ("pid" | int32).as[PgPid] ::
      ("key" | int32).as[PgKey]
    }.as[BackendKeyData]
  }

  private val parameterDescription: Codec[ParameterDescription] = pgHeadlessMsg {
    vectorOfN(
      "param_count" | int16,
      "param_type_id" | oid
    ).withContext("param_descs")
     .as[ParameterDescription]
  }

  private def error(implicit charset: Charset): Codec[StatusMessage.Error] = {
    status(StatusMessage.error).withContext("error_status")
  }

  private def notice(implicit charset: Charset): Codec[StatusMessage.Notice] = {
    status(StatusMessage.notice).withContext("notice_status")
  }

  private def status[A <: StatusMessage](creator: Map[Byte, String] => A)(implicit charset: Charset): Codec[A] = {
    pgHeadlessMsg {
      pgParamMap("param_key" | byte).exmap[A](
        map => Attempt.successful(creator(map)),
        _   => Attempt.failure(Err("encoding not supported"))
      ).withContext("status_msg_params")
    }
  }

  private def notificationResponse(implicit charset: Charset): Codec[NotificationResponse] = {
    pgHeadlessMsg {
      {
        ("pid"     | int32).as[PgPid] ::
        ("channel" | stringNul) ::
        ("payload" | maybeStringNul)
      }.as[NotificationResponse]
    }
  }
}
