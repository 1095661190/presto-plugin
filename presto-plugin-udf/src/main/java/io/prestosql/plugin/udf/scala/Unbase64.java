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
package io.prestosql.plugin.udf.scala;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import org.apache.commons.codec.binary.Base64;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Unbase64
{
    private Unbase64()
    {
    }

    @Description(value = "Decode base64 string")
    @ScalarFunction
    @SqlType(value = "varchar")
    public static Slice unbase64(@SqlType(value = "varchar") Slice par)
    {
        if (par == null) {
            return null;
        }
        try {
            String decodeSting = new String(Base64.decodeBase64((byte[]) par.toStringUtf8().getBytes("UTF-8")));
            return Slices.copiedBuffer((String) decodeSting, (Charset) StandardCharsets.UTF_8);
        }
        catch (UnsupportedEncodingException e) {
            return null;
        }
    }
}
