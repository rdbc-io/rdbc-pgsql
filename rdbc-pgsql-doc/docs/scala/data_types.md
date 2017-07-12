<!---
 ! Copyright 2016-2017 rdbc contributors
 !
 ! Licensed under the Apache License, Version 2.0 (the "License");
 ! you may not use this file except in compliance with the License.
 ! You may obtain a copy of the License at
 !
 !     http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing, software
 ! distributed under the License is distributed on an "AS IS" BASIS,
 ! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 ! See the License for the specific language governing permissions and
 ! limitations under the License. 
 -->
!!! warning
    rdbc-pgsql project and this documentation is still a work in progress.
    It's not ready yet for production use.

General type mapping is described in rdbc documentation
[here](http://docs.api.rdbc.io/scala/types/data_types.md). Said chapter refers
to the types by their standard names. This section describes type mapping using
PostgreSQL type names.

## Type mapping

Following table lists mapping between Scala and supported PostgreSQL types.

| PostgreSQL name           | Standard name            | Scala type   |
|---------------------------|--------------------------|--------------|
| char                      | CHAR/NCHAR               | `String`     |
| varchar                   | VARCHAR/NVARCHAR         | `String`     |
| text                      | CLOB                     | `String`     |
| bytea                     | BLOB                     | `Array[Byte]`|
| bool                      | BOOLEAN                  | `Boolean`|
| numeric                   | NUMERIC                  | [`io.rdbc.sapi.SqlNumeric`]()|
| float4                    | REAL                     | `Float`|
| float8                    | FLOAT                    | `Double`|
| int2                      | SMALLINT                 | `Short`|
| int4                      | INTEGER                  | `Int`|
| int8                      | BIGINT                   | `Long`|
| date                      | DATE                     | `java.time.LocalDate`|
| time                      | TIME                     | `java.time.LocalTime`|
| timestamp                 | TIMESTAMP                | `java.time.LocalDateTime` |
| timestamp with time zone  | TIMESTAMP WITH TIME ZONE | `java.time.Instant`|
| uuid                      | *none*                   | `java.util.UUID` |
