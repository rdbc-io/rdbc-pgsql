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

The table below lists SQL state codes and exceptions that map to them. Every
driver exception class extends some rdbc exception type. You can extract
information specific to PostgreSQL from the driver-specific exceptions.

| SQL state<br>(X means any digit)  | Driver exception type               | rdbc exception type            |
|------------|-------------------------------------|--------------------------------|
| 42501      | `PgUnauthorizedException`           | `UnauthorizedException`        |
| 57014      | `PgTimeoutException`                | `TimeoutException`             |
| 28XXX      | `PgAuthFailureException`            | `AuthFailureException`         |
| 42XXX      | `PgInvalidQueryException`           | `InvalidQueryException`        |
| 23XXX      | `PgConstraintViolationException`    | `ConstraintViolationException` |
| *any other*| `PgUncategorizedStatusDataException`| `UncategorizedRdbcException`   |

The driver also defines a number of exceptions that don't map to any SQL state
codes. For complete list see [the scaladoc]().
