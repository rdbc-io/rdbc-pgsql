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

## What is rdbc-pgsql?

rdbc-pgsql is a [netty](http://netty.io) based [rdbc](http://rdbc.io) driver
for PostgreSQL allowing asynchronous communication with the database in Scala
and Java languages.

## rdbc API

You'll be using this PostgreSQL driver via the database vendor agnostic API which
usage is not covered by this documentation. This guide describes things that
are specific to the driver itself. Please head to the [API documentation site](http://docs.api.rdbc.io)
for detailed information on API usage and general rdbc concepts.

## Supported PostrgreSQL versions

The driver aims to support three latest stable PostgreSQL versions. Currently
it means supporting PostgreSQL 9.6.x, 9.5.x and 9.4.x. The driver is tested
against only these three versions but in fact, it should work with all versions 
since 7.4.

## Getting help

Join [rdbc-io/rdbc](https://gitter.im/rdbc-io/rdbc) gitter channel for 
questions and any kind of discussion about rdbc and the driver.

See also [rdbc](https://stackoverflow.com/questions/tagged/rdbc)
tag on StackOverflow.

## License

rdbc-pgsql is an open source software licensed under
[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
