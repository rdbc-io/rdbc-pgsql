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

General type mapping is described in rdbc documentation
[here](http://docs.api.rdbc.io/scala/types/data_types.md). Said chapter refers
to the types by their standard names. This section describes type mapping using
PostgreSQL type names.

## Type mapping

Following table lists mapping between Scala and supported PostgreSQL types.

| PostgreSQL name           | Scala type   |
|---------------------------|--------------|
| char                      | `String`     |
| varchar                   | `String`     |
| text                      | `String`     |
| bytea                     | `scodec.bits.ByteVector`|
| bool                      | `Boolean`|
| numeric                   | [`io.rdbc.sapi.DecimalNumber`]()|
| float4                    | `Float`|
| float8                    | `Double`|
| int2                      | `Short`|
| int4                      | `Int`|
| int8                      | `Long`|
| date                      | `java.time.LocalDate`|
| time                      | `java.time.LocalTime`|
| timestamp                 | `java.time.Instant` |
| timestamp with time zone  | `java.time.ZonedDateTime`|
| uuid                      | `java.util.UUID` |
