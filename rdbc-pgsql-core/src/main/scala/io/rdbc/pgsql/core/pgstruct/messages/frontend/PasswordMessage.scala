/*
 * Copyright 2016-2017 Krzysztof Pado
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

package io.rdbc.pgsql.core.pgstruct.messages.frontend

import java.security.MessageDigest

import scodec.bits.ByteVector

object PasswordMessage {

  def md5(username: String, password: String, salt: ByteVector): PasswordMessage = {
    //TODO optimize this happy data copying
    val md5 = MessageDigest.getInstance("MD5")
    md5.update(password.getBytes("US-ASCII"))
    md5.update(username.getBytes("US-ASCII"))

    val digest: Array[Byte] = md5.digest()
    val hexBytes = bytesToHex(digest).getBytes("US-ASCII")
    md5.update(hexBytes)
    md5.update(salt.toArray)

    val digest2 = md5.digest()

    val hex2 = bytesToHex(digest2)

    val md5String = "md5" + hex2
    val credentials = Array.concat(md5String.getBytes("US-ASCII"), Array(0.toByte)) //TODO optimize

    PasswordMessage(ByteVector.view(credentials))
  }

  private def bytesToHex(bytes: Array[Byte]): String = {
    bytes.map("%02x" format _).mkString
  }
}

case class PasswordMessage(credentials: ByteVector) extends PgFrontendMessage
