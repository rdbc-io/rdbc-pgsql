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

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigValueFactory._
import com.typesafe.config.{Config, ConfigFactory}
import io.rdbc.api.exceptions.ConnectException.{AuthFailureException, UncategorizedConnectException}
import io.rdbc.pgsql.codec.{DecoderFactory, EncoderFactory}
import io.rdbc.pgsql.core._
import io.rdbc.pgsql.core.auth.{Authenticator, UsernamePasswordAuthenticator}
import io.rdbc.pgsql.core.messages.frontend.StartupMessage
import io.rdbc.pgsql.exception.PgConnectEx
import io.rdbc.pgsql.session.fsm.PgSession
import io.rdbc.pgsql.session.fsm.PgSession.Msg.Inbound.StartSession
import io.rdbc.pgsql.session.fsm.PgSession.Msg.Outbound.{PgSessionError, Ready}
import io.rdbc.sapi.{Connection, ConnectionFactory, TypeConverterRegistry}
import io.rdbc.typeconv.BuiltInConverters

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, FiniteDuration}

object PgConnectionFactory {

  private def baseConfig = ConfigFactory.load().getConfig("adbc.pgsql")

  def apply(): PgConnectionFactory = {
    apply(baseConfig)
  }

  def apply(host: String, port: Int, dbuser: String, database: String, username: String, password: String): PgConnectionFactory = {
    import scala.collection.JavaConversions._

    apply(baseConfig
      .withValue("host", fromAnyRef(host))
      .withValue("port", fromAnyRef(port))
      .withValue("dbuser", fromAnyRef(dbuser))
      .withValue("database", fromAnyRef(database))
      .withValue("authenticator", fromMap(//TODO this is wrong on many levels :) don't fill config props, this should be the other way around
        Map(
          "class" -> classOf[UsernamePasswordAuthenticator].getName,
          "username" -> username,
          "password" -> password
        ))
      )
    )
  }

  def apply(config: Config): PgConnectionFactory = {
    val actorSystemConfig = config.getConfig("actor-system")
    val actorSystem = ActorSystem(actorSystemConfig.getString("name"), actorSystemConfig)

    val decoderFactory = config.getClassInstance[DecoderFactory]("message-decoder-factory")
    val encoderFactory = config.getClassInstance[EncoderFactory]("message-encoder-factory")

    val connectionTimeout = config.getDuration("connection-timeout")

    val host = config.getString("host")
    val port = config.getInt("port")

    val dbuser = config.getString("dbuser")
    val database = config.getString("database")

    val authenticatorConf = config.getConfig("authenticator")
    val authenticator = authenticatorConf.getClassInstance[Authenticator]("class", authenticatorConf)

    val pgTypeConvRegistry = PgTypeConvRegistry.apply(config.getClassInstances[PgTypeConv]("type-converters"): _*)

    val maybeOptions = config.getOptionalConfig("options")

    //TODO SSL

    new PgConnectionFactory(
      actorSystem = actorSystem,
      decoderFactory = decoderFactory,
      encoderFactory = encoderFactory,
      connectionTimeout = Duration.fromNanos(connectionTimeout.toNanos),
      host = host,
      port = port,
      dbuser = dbuser,
      database = database,
      authenticator = authenticator,
      rdbcTypeConvRegistry = BuiltInConverters, //TODO
      pgTypeConvRegistry = pgTypeConvRegistry
    )

  }

}

class PgConnectionFactory(
                           val actorSystem: ActorSystem,
                           val decoderFactory: DecoderFactory,
                           val encoderFactory: EncoderFactory,
                           val connectionTimeout: FiniteDuration,
                           val host: String,
                           val port: Int,
                           val dbuser: String,
                           val database: String,
                           val authenticator: Authenticator,
                           rdbcTypeConvRegistry: TypeConverterRegistry,
                           val pgTypeConvRegistry: PgTypeConvRegistry
                         ) extends ConnectionFactory {

  implicit val implActorSystem = actorSystem
  implicit val materializer = ActorMaterializer()
  implicit val dispatcher = actorSystem.dispatcher

  val typeConverterRegistry = rdbcTypeConvRegistry

  override def connection(): Future[Connection] = {
    val encoder = encoderFactory.encoder()
    val decoder = decoderFactory.decoder()
    val sessionActor = actorSystem.actorOf(PgSession.props(encoder, decoder, authenticator), name = "pgSession")

    val address = InetSocketAddress.createUnresolved(host, port)
    implicit val timeout = akka.util.Timeout.durationToTimeout(connectionTimeout)


    ask(
      sessionActor,
      StartSession(address, StartupMessage(user = dbuser, database = database))
    ).recover {
      case ex => throw UncategorizedConnectException("dupa") //TODO error
    }.flatMap {
      case Ready(_) =>
        implicit val sref = SessionRef(sessionActor)
        Future.successful(new PgConnection(rdbcTypeConvRegistry, pgTypeConvRegistry))

      case PgSessionError.Auth(msg) => Future.failed(AuthFailureException(msg))

      case PgSessionError.PgReported(errMsg) => Future.failed(PgConnectEx(errMsg.statusData))

      case PgSessionError.Uncategorized(msg) => Future.failed(UncategorizedConnectException(msg))

      case any => Future.failed(UncategorizedConnectException(s"Unexpected message '$any' received")) //TODO this probably should be a fatal exception, not an error
    }
  }
}
