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

package io.rdbc.pgsql.core.internal

import io.rdbc.pgsql.core.internal.typecodec.sco._
import io.rdbc.pgsql.core.types._

package object typecodec {

  private[core] val BuiltInCodecs: Vector[ScodecPgValCodec[_ <: PgVal[_]]] = Vector(
    ScodecPgBoolCodec,
    ScodecPgByteaCodec,
    ScodecPgCharCodec,
    ScodecPgDateCodec,
    ScodecPgFloat4Codec,
    ScodecPgFloat8Codec,
    ScodecPgInt2Codec,
    ScodecPgInt4Codec,
    ScodecPgInt8Codec,
    ScodecPgNumericCodec,
    ScodecPgTextCodec,
    ScodecPgTimeCodec,
    ScodecPgTimestampCodec,
    ScodecPgTimestampTzCodec,
    ScodecPgUuidCodec,
    ScodecPgVarcharCodec
  )
}
