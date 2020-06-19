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
import io.prestosql.spi.type.StandardTypes;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class IpLookup
{
//    private static final String COUNTRY_DATA_FILE = "/usr/share/GeoIP/GeoIP.dat";
//    private static final String CITY_DATA_FILE = "/usr/share/GeoIP/GeoIPCity.dat";

//    private static final LookupService countryLookup = createLookup("/usr/share/GeoIP/GeoIP.dat");
//    private static final LookupService cityLookup = createLookup("/usr/share/GeoIP/GeoIPCity.dat");

//      private static final LookupService countryLookup = createLookup("/Users/happyelements/Documents/workspace/presto-plugin/presto-plugin-udf/src/main/resources/GeoIP.dat");
//    private static final LookupService cityLookup = createLookup("/Users/happyelements/Documents/workspace/presto-plugin/presto-plugin-udf/src/main/resources/GeoIPCity.dat");

    private static final LookupService countryLookup ;

    private static final LookupService cityLookup ;


    static {
        try {
            //当前进程id：
            String pid = ManagementFactory.getRuntimeMXBean().getName();
            Long tid = Thread.currentThread().getId();
//            logger.error("now container pid is {} tid is {}", pid, tid);
            File file_dir = new File(File.separator + "tmp" + File.separator + System.getProperty("user.name"));
            file_dir.mkdirs();
            File tmp_lastCountryDatFile = new File(file_dir, System.currentTimeMillis() + "." + pid + "." + tid + "GeoIP.dat");
            File tmp_lastCityDatFile = new File(file_dir, System.currentTimeMillis() + "." + pid + "." + tid + "GeoIPCity.dat");
            RandomAccessFile country_file = new RandomAccessFile(tmp_lastCountryDatFile, "rw");
            RandomAccessFile city_file = new RandomAccessFile(tmp_lastCityDatFile, "rw");
            InputStream inCountry = IpLookup.class.getClassLoader().getResourceAsStream("GeoIP.dat");
            byte[] buffer = new byte[8096];
            int offset = 0;
            while ((offset = inCountry.read(buffer, 0, buffer.length)) > 0) {
                country_file.write(buffer, 0, offset);
            }
            inCountry.close();
            InputStream inCity = IpLookup.class.getClassLoader().getResourceAsStream("GeoIPCity.dat");
            buffer = new byte[8096];
            offset = 0;
            while ((offset = inCity.read(buffer, 0, buffer.length)) > 0) {
                city_file.write(buffer, 0, offset);
            }
            inCity.close();
            buffer = null;
            tmp_lastCityDatFile.delete();
            tmp_lastCountryDatFile.delete();

            country_file.seek(0);
            city_file.seek(0);
            countryLookup = createLookup(country_file);
            cityLookup = createLookup(city_file);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    private IpLookup()
    {
    }

/*    private static LookupService createLookup(String dataFile)
    {
        try {
            return new LookupService(new File(dataFile));
        }
        catch (IOException e) {
            throw new RuntimeException("counld not find Geo data file: " + dataFile, e);
        }
    }*/

    private static LookupService createLookup(RandomAccessFile dataFile) {
        try {
            return new LookupService(dataFile);
        } catch (IOException e) {
            throw new RuntimeException("counld not find Geo data file: " + dataFile, e);
        }
    }

    /*@Description(value = "country or city lookup from ip")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    */
    public static Slice geoip(@SqlType(StandardTypes.VARCHAR) Slice category, @SqlType(StandardTypes.VARCHAR) Slice ip)
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
            default:
                return Slices.copiedBuffer("bi_null", (Charset) StandardCharsets.UTF_8);
        }
    }
}
