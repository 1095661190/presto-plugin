/*
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
package io.prestosql.plugin.udf;

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.udf.scala.*;
import io.prestosql.spi.Plugin;

import java.util.Set;

public class UdfPlugin
        implements Plugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        /*
         * Presto 0.157 does not expose the interfaces to add SqlFunction objects directly
         * We can only add udfs via Annotations now
         *
         * Unsupported udfs right now:
         * Hash
         * Nvl
         * array_aggr
         */
        return ImmutableSet.<Class<?>>builder()
                .add(CastTimeZone.class)
                .add(Unbase64.class)
                .add(GeoIP2.class)
                .build();
    }
}
