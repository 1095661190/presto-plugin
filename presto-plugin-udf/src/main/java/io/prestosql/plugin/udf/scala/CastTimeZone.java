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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;

public class CastTimeZone
{
    static SimpleDateFormat srcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static HashMap<String, SimpleDateFormat> toFormats;

    private CastTimeZone()
    {
    }

    @Description(value = "Converts time to the timezone")
    @ScalarFunction
    @SqlType(value = "varchar")
    public static Slice ctimezone(@SqlType(value = "varchar") Slice datetime, @SqlType(value = "varchar") Slice timeZone2)
    {
        if (datetime == null || timeZone2 == null) {
            return null;
        }
        SimpleDateFormat toFormat = toFormats.get(timeZone2.toStringUtf8());
        if (toFormat == null) {
            return datetime;
        }
        try {
            Date midDate = srcFormat.parse(datetime.toStringUtf8());
            toFormat.format(midDate);
            return Slices.copiedBuffer((String) toFormat.format(midDate), (Charset) StandardCharsets.UTF_8);
        }
        catch (ParseException e) {
            return null;
        }
    }

    static {
        srcFormat.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        toFormats = new HashMap();
        SimpleDateFormat cetFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        cetFormat.setTimeZone(TimeZone.getTimeZone("CET"));
        toFormats.put("CET", cetFormat);
        SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        toFormats.put("UTC", utcFormat);
        SimpleDateFormat pdtFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        pdtFormat.setTimeZone(TimeZone.getTimeZone("PST8PDT"));
        toFormats.put("PST8PDT", pdtFormat);
    }
}
