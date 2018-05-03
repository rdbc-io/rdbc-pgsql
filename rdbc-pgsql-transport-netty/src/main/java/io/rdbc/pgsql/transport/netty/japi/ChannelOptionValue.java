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

import io.netty.channel.ChannelOption;
import org.immutables.value.Value.Parameter;
import org.immutables.value.Value.Style.ImplementationVisibility;

import static org.immutables.value.Value.Immutable;
import static org.immutables.value.Value.Style;

@Immutable
@Style(visibility = ImplementationVisibility.PACKAGE)
public interface ChannelOptionValue<T> {

    @Parameter
    ChannelOption<T> getOption();

    @Parameter
    T getValue();

    static <U> ChannelOptionValue<U> of(ChannelOption<U> option, U value) {
        return ImmutableChannelOptionValue.of(option, value);
    }

}
