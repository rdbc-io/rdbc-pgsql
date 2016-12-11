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

import _root_.scodec.Codec
import _root_.scodec.codecs._
import io.rdbc.pgsql.core.messages.backend._
import io.rdbc.pgsql.core.messages.backend.auth.{AuthBackendMessage, AuthOk, AuthRequestMd5}
import io.rdbc.pgsql.scodec._

package object backend {


  def pgBackendMessage(implicit charset: Charset): Codec[PgBackendMessage] = {
    //TODO creating this discriminator list for each backend msg is a major bottleneck, cache for each charset?
    discriminatorFallback(
      left = unknown,
      right = discriminated[PgBackendMessage].by(byte)
        .typecase('S', parameterStatus)
        .typecase('R', auth)
        .typecase('Z', readyForQuery)
        .typecase('T', rowDescription)
        .typecase('D', dataRow)
        .typecase('C', commandComplete)
        .typecase('K', backendKeyData)
        .typecase('1', parseComplete)
        .typecase('2', bindComplete)
        .typecase('3', closeComplete)
        .typecase('I', emptyQueryResponse)
        .typecase('E', error)
        .typecase('N', notice)
        .typecase('s', portalSuspended)
        .typecase('n', noData)
        .typecase('t', parameterDescription)
    ).xmapc(_.fold(identity, identity)) {
      case u: UnknownBackendMessage => Left(u)
      case r => Right(r)
    }
  }

  val noData: Codec[NoData.type] = pgSingletonMsg(NoData)

  val closeComplete: Codec[CloseComplete.type] = pgSingletonMsg(CloseComplete)

  val portalSuspended: Codec[PortalSuspended.type] = pgSingletonMsg(PortalSuspended)

  val parseComplete: Codec[ParseComplete.type] = pgSingletonMsg(ParseComplete)

  val bindComplete: Codec[BindComplete.type] = pgSingletonMsg(BindComplete)

  val emptyQueryResponse: Codec[EmptyQueryResponse.type] = pgSingletonMsg(EmptyQueryResponse)

  def commandComplete(implicit charset: Charset): Codec[CommandComplete] = pgHeadlessMsg {
    pgString.xmap[CommandComplete](
      message => {
        if (CommandComplete.RowCountMessages.exists(rowCountMsg => message.startsWith(rowCountMsg))) {
          val (constant, rowsStr) = message.splitAt(message.lastIndexOf(" ") + 1)
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

  def parameterStatus(implicit charset: Charset): Codec[ParameterStatus] = pgHeadlessMsg {
    pgParam(pgString).xmap({
      case (key, value) => ParameterStatus(key, value)
    }, p => (p.key, p.value))
  }

  def dataRow(implicit charset: Charset): Codec[DataRow] = pgHeadlessMsg {
    vectorOfN(pgInt16.withContext("columns"), fieldValue).as[DataRow]
  }

  val unknown: Codec[UnknownBackendMessage] = {
    {
      ("head" | byte) ::
        ("body" | variableSizeBytes(pgInt32, bytesArr, 4))
    }.as[UnknownBackendMessage]
  }

  val readyForQuery: Codec[ReadyForQuery] = pgHeadlessMsg {
    discriminated.by(byte)
      .typecase('I', provide(ReadyForQuery(TxStatus.Idle)))
      .typecase('E', provide(ReadyForQuery(TxStatus.Failed)))
      .typecase('T', provide(ReadyForQuery(TxStatus.Active)))
    //TODO error handling
  }

  val authRequestMd5: Codec[AuthRequestMd5] = {
    bytesArr(4).withContext("md5 salt").as[AuthRequestMd5].withToString("AuthRequestMd5")
  }

  val auth: Codec[AuthBackendMessage] = pgHeadlessMsg {
    discriminated[AuthBackendMessage].by(pgInt32)
      .typecase(0, provide[AuthBackendMessage](AuthOk))
      .typecase(5, authRequestMd5) //TODO more auth mechanisms here, TODO on discrimination fail return meaningful err
  }

  def rowDescription(implicit charset: Charset): Codec[RowDescription] = pgHeadlessMsg {
    vectorOfN(pgInt16.withContext("fieldCount"), fieldDescription).as[RowDescription]
  }

  val backendKeyData: Codec[BackendKeyData] = pgHeadlessMsg {
    {
      ("pid" | pgInt32) ::
        ("key" | pgInt32)
    }.as[BackendKeyData]
  }

  val parameterDescription: Codec[ParameterDescription] = pgHeadlessMsg {
    vectorOfN(pgInt16.withContext("paramCount"), oid)
      .as[ParameterDescription]
  }

  def error(implicit charset: Charset): Codec[StatusMessage.Error] = status(StatusMessage.error)

  def notice(implicit charset: Charset): Codec[StatusMessage.Notice] = status(StatusMessage.notice)

  private def status[A <: StatusMessage](creator: Map[Byte, String] => A)(implicit charset: Charset): Codec[A] = pgHeadlessMsg {
    pgParamMap(byte).withContext("fields")
      .xmap(creator,
        msg => ??? /*  {
        msg => msg.fields.map {
          case (fieldType, fieldVal) => (fieldType.code, fieldVal)
        }
      } TODO encoding*/
      )
  }

}
