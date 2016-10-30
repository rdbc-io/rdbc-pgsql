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

package io.rdbc.pgsql.netty

import io.rdbc.ImmutSeq
import io.rdbc.pgsql.core.messages.backend.{RowDescription, StatusMessage}
import io.rdbc.pgsql.core.types.PgTypeRegistry
import io.rdbc.sapi._
import org.reactivestreams.Publisher

import scala.concurrent.{ExecutionContext, Future}

class PgResultStream(val rows: Publisher[Row],
                     rowDesc: RowDescription,
                     val rowsAffected: Future[Long],
                     warningMsgsFut: Future[ImmutSeq[StatusMessage]],
                     pgTypeConvRegistry: PgTypeRegistry,
                     rdbcTypeConvRegistry: TypeConverterRegistry) extends ResultStream {

  implicit val ec = ExecutionContext.global //TODO

  val warnings: Future[ImmutSeq[Warning]] = warningMsgsFut.map { warnMsgs =>
    warnMsgs.map(warnMsg => Warning(warnMsg.statusData.shortInfo, warnMsg.statusData.sqlState))
  }

  lazy val metadata: RowMetadata = {
    val columnsMetadata = rowDesc.fieldDescriptions.map { fdesc =>
      ColumnMetadata(fdesc.name, fdesc.dataType.oid.code.toString, pgTypeConvRegistry.oid2type.get(fdesc.dataType.oid).map(_.cls))
    }
    RowMetadata(columnsMetadata)
  }
}
