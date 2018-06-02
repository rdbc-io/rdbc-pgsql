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

The driver supports PostgreSQL authentication methods with [`Authenticator`]()
implementations it provides. `Authenticator` instances can be obtained
using methods of [`Auth`]() factory object and can be passed when creating
connection factory configuration as follows:

```scala hl_lines="4"
val config = NettyPgConnectionFactory.Config(
       host = "pg.example.com",
       port = 5432,
       authenticator = Auth.password("user", "pass"),
       /* other options */
      )
```

## Provided authenticators

-    **PasswordAuthenticator**

     Factory method: `:::scala Auth.password`
     
     This authenticator covers `password` and `md5` authentication methods
     described in PostgreSQL documentation [here](https://www.postgresql.org/docs/current/static/auth-methods.html#AUTH-PASSWORD).
