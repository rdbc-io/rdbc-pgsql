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

package io.rdbc.pgsql.core.internal.protocol

final case class StatusData(severity: String,
                            sqlState: String,
                            message: String,
                            detail: Option[String],
                            hint: Option[String],
                            position: Option[Int],
                            internalPosition: Option[Int],
                            internalQuery: Option[String],
                            where: Option[String],
                            schemaName: Option[String],
                            tableName: Option[String],
                            columnName: Option[String],
                            dataTypeName: Option[String],
                            constraintName: Option[String],
                            file: String,
                            line: String,
                            routine: String) {

  def shortInfo: String = s"$severity-$sqlState: $message"
}
