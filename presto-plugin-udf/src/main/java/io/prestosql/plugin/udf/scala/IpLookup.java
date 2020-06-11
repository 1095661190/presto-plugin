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

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.regionName;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class IpLookup
{
    private static final String COUNTRY_DATA_FILE = "/usr/share/GeoIP/GeoIP.dat";
    private static final String CITY_DATA_FILE = "/usr/share/GeoIP/GeoIPCity.dat";
    private static final LookupService countryLookup = createLookup("/usr/share/GeoIP/GeoIP.dat");
    private static final LookupService cityLookup = createLookup("/usr/share/GeoIP/GeoIPCity.dat");

    private IpLookup()
    {
    }

    private static LookupService createLookup(String dataFile)
    {
        try {
            return new LookupService(new File(dataFile));
        }
        catch (IOException e) {
            throw new RuntimeException("counld not find Geo data file: " + dataFile, e);
        }
    }

    @Description(value = "country or city lookup from ip")
    @ScalarFunction
    @SqlType(value = "varchar")
    public static Slice geoip(@SqlType(value = "varchar") Slice category, @SqlType(value = "varchar") Slice ip)
    {
        if (category == null || ip == null) {
            return null;
        }
        String categoryStr = category.toStringUtf8();
        switch (categoryStr.toLowerCase()) {
            case "country": {
                return Slices.copiedBuffer((String) countryLookup.getCountry(ip.toStringUtf8()).getName(), (Charset) StandardCharsets.UTF_8);
            }
            case "city": {
                Location location = cityLookup.getLocation(ip.toStringUtf8());
                if (location == null) {
                    return Slices.copiedBuffer((String) "bi_null", (Charset) StandardCharsets.UTF_8);
                }
                return Slices.copiedBuffer((String) (location.city == null ? "bi_null" : location.city), (Charset) StandardCharsets.UTF_8);
            }
            case "latitude": {
                Location l0 = cityLookup.getLocation(ip.toStringUtf8());
                return Slices.copiedBuffer((String) (l0 == null ? "" + -1 : "" + l0.latitude), (Charset) StandardCharsets.UTF_8);
            }
            case "longitude": {
                Location l1 = cityLookup.getLocation(ip.toStringUtf8());
                return Slices.copiedBuffer((String) (l1 == null ? "" + -1 : "" + l1.longitude), (Charset) StandardCharsets.UTF_8);
            }
            case "region": {
                Location l11 = cityLookup.getLocation(ip.toStringUtf8());
                String region = "bi_null";
                if (l11 != null) {
                    String localRegion = regionName.regionNameByCode(l11.countryCode, l11.region);
                    region = localRegion == null ? "bi_null" : localRegion;
                }
                return Slices.copiedBuffer((String) region, (Charset) StandardCharsets.UTF_8);
            }
        }
        return null;
    }
}
