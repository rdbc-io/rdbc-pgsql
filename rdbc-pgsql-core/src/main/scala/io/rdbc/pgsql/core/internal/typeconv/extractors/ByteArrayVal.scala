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

package io.rdbc.pgsql.core.internal.typeconv.extractors

import io.rdbc.japi

private[typeconv] object ByteArrayVal {
  def unapply(any: Any): Option[Array[Byte]] = {
    any match {
      case bytes: Array[Byte] => Some(bytes)
      case sb: japi.SqlBlob => Some(sb.getValue)
      case sb: japi.SqlBinary => Some(sb.getValue)
      case _ => None
    }
  }
}
