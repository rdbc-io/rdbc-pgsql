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

package io.rdbc.pgsql.core.internal

import io.rdbc.ImmutSeq
import io.rdbc.pgsql.core.pgstruct.messages.backend.{RowDescription, StatusMessage}
import io.rdbc.pgsql.core.types.PgTypeRegistry
import io.rdbc.sapi._
import io.rdbc.util.Logging
import org.reactivestreams.Publisher

import scala.concurrent.{ExecutionContext, Future}

private[core] class PgResultStream(val rows: Publisher[Row],
                                   rowDesc: RowDescription,
                                   val rowsAffected: Future[Long],
                                   warningMsgsFut: Future[Vector[StatusMessage]],
                                   pgTypes: PgTypeRegistry,
                                   typeConverters: TypeConverterRegistry)
                                  (implicit ec: ExecutionContext)
  extends ResultStream
    with Logging {

  lazy val warnings: Future[ImmutSeq[Warning]] = traced {
    warningMsgsFut.map { warnMsgs =>
      warnMsgs.map { warnMsg =>
        Warning(warnMsg.statusData.shortInfo, warnMsg.statusData.sqlState)
      }
    }
  }

  lazy val metadata: RowMetadata = traced {
    val columnsMetadata = rowDesc.colDescs.map { colDesc =>
      ColumnMetadata(
        name = colDesc.name.value,
        dbTypeId = colDesc.dataType.oid.value.toString,
        cls = pgTypes.typeByOid(colDesc.dataType.oid).map(_.cls)
      )
    }
    RowMetadata(columnsMetadata)
  }
}
