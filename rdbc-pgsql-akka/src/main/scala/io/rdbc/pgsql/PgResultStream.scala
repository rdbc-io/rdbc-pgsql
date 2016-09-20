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

package io.rdbc.pgsql

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import io.rdbc.ImmutSeq
import io.rdbc.pgsql.core.{PgRow, PgTypeConvRegistry}
import io.rdbc.pgsql.session.fsm.PgSession.Msg.Outbound.SourceRef
import io.rdbc.sapi._
import org.reactivestreams.Publisher

import scala.concurrent.{ExecutionContext, Future}

class PgResultStream(sourceRef: SourceRef,
                     val rdbcTypeConvRegistry: TypeConverterRegistry,
                     val pgTypeConvRegistry: PgTypeConvRegistry)(implicit materializer: ActorMaterializer, ec: ExecutionContext)
  extends ResultStream {

  val rowsAffected: Future[Long] = sourceRef.rowsAffected //TODO combine this with rows affected

  val warnings: Future[ImmutSeq[Warning]] = sourceRef.warnings.map { warnMsgs =>
    warnMsgs.map(warnMsg => Warning(warnMsg.statusData.shortInfo, warnMsg.statusData.sqlState))
  }

  val rows: Publisher[Row] = {
    val nameMappings = Map(sourceRef.rowDesc.fieldDescriptions.zipWithIndex.map {
      case (fdesc, idx) => fdesc.name -> idx
    }: _*
    )

    sourceRef.source.map { dr =>
      new PgRow(sourceRef.rowDesc, dr.fieldValues, nameMappings, rdbcTypeConvRegistry, pgTypeConvRegistry)
    }.runWith(Sink.asPublisher(fanout = false))
  }

  lazy val metadata: RowMetadata = {
    val columnsMetadata = sourceRef.rowDesc.fieldDescriptions.map { fdesc =>
      ColumnMetadata(fdesc.name, fdesc.dataType.oid.code.toString, pgTypeConvRegistry.oid2conv.get(fdesc.dataType.oid).map(_.primaryClass))
    }
    RowMetadata(columnsMetadata)
  }
}
