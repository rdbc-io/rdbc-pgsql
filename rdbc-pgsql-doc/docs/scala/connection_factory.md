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

As rdbc documentation [mentions](http://docs.api.rdbc.io/scala/connection/#connecting-to-a-database),
to use the API you need to construct a driver specific [`ConnectionFactory`]() 
implementation. In rdbc-pgsql case, it's going to be
[`NettyPgConnectionFactory`]().

To instantiate `NettyPgConnectionFactory` use its companion object's `apply`
method. This factory method accepts a configuration object &mdash; 
[`NettyPgConnFactory#Config`]() instance. Chapters below focus on providing
the configuration.

## Simple scenario

To create a configuration instance with default configuration options
use `NettyPgConnFactory#Config`'s `apply` method with `host`, `port` and
`authenticator` parameters:
 
```scala
import io.rdbc.pgsql.transport.netty.NettyPgConnectionFactory
import io.rdbc.pgsql.transport.netty.NettyPgConnectionFactory.Config
import io.rdbc.pgsql.core.auth.Auth

val cf = NettyPgConnectionFactory(
  Config(
    host = "localhost",
    port = 5432,
    authenticator = Auth.password("user", "pass")
  )
)
```
 
If you want to tweak some configuration options (and most likely you do) read
the paragraph below.

## Configuration options

If you want to override some of default option values use named parameters
of `Config.apply` method. In the example below `dbName` option is changed
from default `None` to `Some("db")`. 

```scala
import io.rdbc.pgsql.transport.netty.NettyPgConnectionFactory
import io.rdbc.pgsql.transport.netty.NettyPgConnectionFactory.Config
import io.rdbc.pgsql.core.auth.Auth

val cf = NettyPgConnectionFactory(Config(
  host = "localhost",
  port = 5432,
  authenticator = Auth.password("user", "pass"),
  dbName = Some("db")
))
```

For other examples see [examples]() paragraph.

The paragraphs below list available configuration options.

### Non-transport specific options

The driver is designed to support multiple transport libraries. This paragraph lists
options non-specific to any transport library.

#### Frequently used

-    **host**

     Host name or IP address the database listens on.
     
     Default value: no default value.

---

-    **port**

     TCP port the database listens on.
     
     Default value: no default value.

---

-    **authenticator**
     
     [`Authenticator`]() instance performing authentication. See
     [authentication](authentication.md) chapter for details.
     
     Default value: no default value.

---

-    **dbName**
     
     The database name. if `None` is provided database name will be assumed to
     be the same as user name provide by the authenticator.
     
     Default value: `:::scala None`.

---

-    **subscriberBufferCapacity**
     
     When streaming data from client to the database the driver can accumulate
     statements into batches for optimized execution. This parameter sets maximum
     number of statements that will be accumulated.
     
     Default value: `:::scala 100`.

---

-    **stmtCacheConfig**
     
     Connection can cache prepared statements so that they can be reused when
     executed later on with different parameters. This paramaeter configures
     caching mechanism.
     
     Possible values:
     
     -    `:::scala StmtCacheConfig.Disabled` &mdash; disables caching
     -    `:::scala StmtCacheConfig.Enabled` &mdash; enables caching. `Enabled` case class
           has following fields:
           
           -    **capacity**
           
               Specifies maximum prepared statements that can be cached.
     
     Default value: `:::scala Enabled(capacity = 100)`.

---

-    **writeTimeout**
     
     If socket write operation takes longer than this value, timeout error will be reported.
     
     Default value: `:::scala Timeout(10.seconds)`.

---

-    **ec**
     
     Execution context used by the connection to execute `Future` callbacks.
     
     Default value: `:::scala scala.concurrent.ExecutionContext.global`.

#### Other

-    **typeConvertersProviders**
     
     Providers providing converters that can do conversions between Scala types
     that represent database values and any other types. This option
     may be useful if you want to use Scala types not supported by default by rdbc.
     By default, conversions listed [here]() are provided.
     
     Default value: `:::scala Vector(new StandardTypeConvertersProvider)`.

---

-    **pgTypesProviders**
     
     Providers providing definitions of PostgreSQL types and means to serialize
     and deserialize them. By default, types listed [here]() are supported
     and [scodec]() library is used for serialization and deserialization.
     
     Default value: `:::scala Vector(new ScodecPgTypesProvider)`.
     
---

-    **msgDecoderFactory**
     
     A factory creating PostgreSQL protocol message decoder. By default,
     [scodec]() library is used to decode messages.
     
     Default value: `:::scala new ScodecDecoderFactory`.
     
---

-    **msgEncoderFactory**
     
     A factory creating PostgreSQL protocol message encoder. By default,
     [scodec]() library is used to encode messages.
     
     Default value: `:::scala new ScodecEncoderFactory`.
     
---

-    **subscriberMinDemandRequestSize**
     
     When streaming data from a client to a database the driver requests statement
     argument sets from the client. This parameter defines minimum number of argument
     sets that the driver will request, each time the driver decides that it needs
     more argument sets.
     
     Default value: `:::scala 10`.
     

### Netty transport specific options   

-    **channelFactory**
     
     Netty [`ChannelFactory`]() that will be used to create a channel.
     
     By default JDK NIO will be used which is supported on all platforms.
     Consider using platform specific native netty transport if your platform
     supports any. See [this](http://netty.io/wiki/native-transports.html)
     page for details.
     
     Default value: `:::scala new NioChannelFactory`

---

-    **eventLoopGroup**
     
     Netty [`EventLoopGroup`]() which will be used to handle events for the channel.
     
     By default JDK NIO will be used which is supported on all platforms.
     Consider using platform specific native netty transport if your platform
     supports any. See [this](http://netty.io/wiki/native-transports.html)
     page for details.
     
     Default value: `:::scala new NioEventLoopGroup`.
          
---

-    **channelOptions**
     
     Netty channel options.
     
     `CONNECT_TIMEOUT_MILLIS` is always ignored because it's overridden by
     timeout value passed to factory's `connection()` method. Other than that,
     you can set any netty channel option here.
     
     Default values:
     
     -    `SO_KEEPALIVE = true`

### Examples

Code in the snippet below creates a connection factory setting transport independent
`writeTimeout` and `subscriberBufferCapacity` options and also setting netty-specific
`channelOptions` option.

```scala

import io.rdbc.pgsql.transport.netty.ChannelOptions._
import io.rdbc.pgsql.transport.netty.NettyPgConnectionFactory
import io.rdbc.pgsql.transport.netty.NettyPgConnectionFactory.Config
import io.rdbc.pgsql.core.auth.Auth
import io.rdbc.sapi.Timeout
import io.netty.channel.ChannelOption._
import scala.concurrent.duration._

val cf = {
  NettyPgConnectionFactory(
    Config(
      host = "pg.example.com",
      port = 5432,
      authenticator = Auth.password("user", "pass"),
      writeTimeout = Timeout(5.seconds),
      subscriberBufferCapacity = 1000,
      channelOptions = Vector(
        SO_KEEPALIVE -> true,
        WRITE_SPIN_COUNT -> 3
      )
    )
  )
}
```
