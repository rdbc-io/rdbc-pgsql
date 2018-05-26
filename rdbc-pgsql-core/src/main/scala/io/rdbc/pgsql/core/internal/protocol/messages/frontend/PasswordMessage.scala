/*
 * Copyright 2016 rdbc contributors
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

package io.rdbc.pgsql.core.internal.protocol.messages.frontend

import java.security.MessageDigest

import scodec.bits.ByteVector

object PasswordMessage {

  private val PasswordEncoding = "US-ASCII"

  def md5(username: String, password: String, salt: ByteVector): PasswordMessage = {
    val md5Digest = MessageDigest.getInstance("MD5")
    md5Digest.update(password.getBytes(PasswordEncoding))
    md5Digest.update(username.getBytes(PasswordEncoding))
    val passUserMd5: Array[Byte] = md5Digest.digest()

    val passUserMd5HexBytes = bytesToHex(passUserMd5).getBytes(PasswordEncoding)
    md5Digest.update(passUserMd5HexBytes)
    md5Digest.update(salt.toArray)
    val finalMd5 = md5Digest.digest()

    val finalMd5String = "md5" + bytesToHex(finalMd5)
    val credentials = Array.concat(finalMd5String.getBytes(PasswordEncoding), Array(0.toByte))

    PasswordMessage(ByteVector.view(credentials))
  }

  def cleartext(password: String): PasswordMessage = {
    val passNulTerminated = Array.concat(password.getBytes(PasswordEncoding), Array(0.toByte))
    PasswordMessage(ByteVector.view(passNulTerminated))
  }

  private def bytesToHex(bytes: Array[Byte]): String = {
    bytes.map("%02x" format _).mkString
  }
}

case class PasswordMessage(credentials: ByteVector) extends PgFrontendMessage
