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

package io.rdbc.pgsql.transport.netty.japi;


import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.rdbc.jadapter.ConnectionFactoryAdapter;
import io.rdbc.japi.ConnectionFactory;
import io.rdbc.japi.TypeConvertersProvider;
import io.rdbc.pgsql.core.auth.Authenticator;
import io.rdbc.pgsql.core.codec.DecoderFactory;
import io.rdbc.pgsql.core.codec.EncoderFactory;
import io.rdbc.pgsql.core.codec.scodec.ScodecDecoderFactory;
import io.rdbc.pgsql.core.codec.scodec.ScodecEncoderFactory;
import io.rdbc.pgsql.core.config.japi.PgTypesProviders;
import io.rdbc.pgsql.core.config.japi.StmtCacheConfig;
import io.rdbc.pgsql.core.types.PgTypesProvider;
import io.rdbc.pgsql.transport.netty.sapi.NioChannelFactory;
import io.rdbc.typeconv.StandardTypeConvertersProvider;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;
import scala.PartialFunction$;
import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

import static org.immutables.value.Value.Default;

public class NettyPgConnectionFactory
        extends ConnectionFactoryAdapter
        implements ConnectionFactory {

    protected NettyPgConnectionFactory(io.rdbc.sapi.ConnectionFactory underlying, ExecutionContext ec) {
        super(underlying, PartialFunction$.MODULE$.empty(), ec); //TODO exceptionConverter
    }

    public static NettyPgConnectionFactory create(Config config) {
        return ScalaToJavaNettyPgConnectionFactory.create(config);
    }

    @Immutable
    @Style(visibility = ImplementationVisibility.PACKAGE, typeImmutable = "ImmutableNettyPgConnFactoryConfig")
    public interface Config {
        String host();

        int port();

        Authenticator authenticator();

        @Default
        default ChannelFactory<? extends Channel> channelFactory() {
            return new NioChannelFactory();
        }

        @Default
        default EventLoopGroup eventLoopGroup() {
            return new NioEventLoopGroup();
        }

        @Default
        default List<ChannelOptionValue<?>> channelOptions() {
            return Collections.singletonList(
                    ChannelOptionValue.of(ChannelOption.SO_KEEPALIVE, true)
            );
        }

        Optional<String> dbName();

        @Default
        default int subscriberBufferCapacity() {
            return 100;
        }

        @Default
        default int subscriberMinDemandRequestSize() {
            return 10;
        }

        @Default
        default StmtCacheConfig cacheConfig() {
            return StmtCacheConfig.builder().enabled(true).capacity(100).build();
        }

        @Default
        default List<PgTypesProvider> pgTypesProviders() {
            return Collections.singletonList(PgTypesProviders.scodec());
        }

        @Default
        default DecoderFactory msgDecoderFactory() {
            return new ScodecDecoderFactory();
        }

        @Default
        default EncoderFactory msgEncoderFactory() {
            return new ScodecEncoderFactory();
        }

        @Default
        default Duration writeTimeout() {
            return Duration.of(10L, ChronoUnit.SECONDS);
        }

        @Default
        default ExecutionContext executionContext() {
            return ExecutionContext$.MODULE$.global();
        }

        static Builder builder() {
            return ImmutableNettyPgConnFactoryConfig.builder();
        }

        interface Builder {
            Builder host(String host);

            Builder port(int port);

            Builder authenticator(Authenticator authenticator);

            Builder dbName(Optional<String> dbName);

            Builder dbName(String dbName);

            Builder subscriberBufferCapacity(int capacity);

            Builder subscriberMinDemandRequestSize(int subscriberMinDemandRequestSize);

            Builder cacheConfig(StmtCacheConfig config);

            Builder pgTypesProviders(Iterable<? extends PgTypesProvider> typeConvertersProviders);

            Builder msgDecoderFactory(DecoderFactory decoderFactory);

            Builder msgEncoderFactory(EncoderFactory encoderFactory);

            Builder writeTimeout(Duration timeout);

            Builder executionContext(ExecutionContext ec);

            @Default
            default Builder executor(Executor executor) {
                return executionContext(ExecutionContext$.MODULE$.fromExecutor(executor));
            }

            Config build();
        }
    }
}
