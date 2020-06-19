package io.prestosql;

import com.maxmind.db.Reader;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.regionName;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.Subdivision;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.plugin.udf.scala.GeoIP2;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class GeoipTest
{
    private static final String CITY_DATA_FILE = "GeoLite2-City.mmdb";
    private static final String COUNTRY_DATA_FILE = "GeoIP2-Country.mmdb";
    private static final Charset charset = StandardCharsets.UTF_8;

//    private static final DatabaseReader country = createDatabaseReader(new File("/Users/happyelements/Documents/workspace/presto-plugin/presto-plugin-udf/src/main/resources/" + COUNTRY_DATA_FILE));
    private static final DatabaseReader city = createDatabaseReader(new File("/Users/happyelements/Documents/workspace/presto-plugin/presto-plugin-udf/src/main/resources/" + CITY_DATA_FILE));

    private static final DatabaseReader country = createDatabaseReader(GeoipTest.class.getClassLoader().getResourceAsStream(COUNTRY_DATA_FILE));

    public static void main(String[] args)
    {
//        String ip = "220.181.38.148";
        //String ip = "172.217.174.110";
        String ip="37.48.206.92"; //Syria
        Charset charset = StandardCharsets.UTF_8;
        System.out.println(geoip(Slices.copiedBuffer("country", charset), Slices.copiedBuffer(ip, charset)).toStringUtf8());
        System.out.println(geoip(Slices.copiedBuffer("city", charset), Slices.copiedBuffer(ip, charset)).toStringUtf8());
        System.out.println(geoip(Slices.copiedBuffer("latitude", charset), Slices.copiedBuffer(ip, charset)).toStringUtf8());
        System.out.println(geoip(Slices.copiedBuffer("longitude", charset), Slices.copiedBuffer(ip, charset)).toStringUtf8());
        System.out.println(geoip(Slices.copiedBuffer("region", charset), Slices.copiedBuffer(ip, charset)).toStringUtf8());
    }

    /**
     * @param dataFileIn #File/InputStream dataFileIn
     * @return
     */
    private static DatabaseReader createDatabaseReader(InputStream dataFileIn)
    {
        try {
            DatabaseReader.Builder builder = new DatabaseReader.Builder(dataFileIn);
            builder.fileMode(Reader.FileMode.MEMORY);
            return builder.build();
        }
        catch (IOException e) {
            throw new RuntimeException("counld not find Geo2  data file: ", e);
        }
    }

    private static DatabaseReader createDatabaseReader(File dataFileIn)
    {
        try {
            DatabaseReader.Builder builder = new DatabaseReader.Builder(dataFileIn);
            builder.fileMode(Reader.FileMode.MEMORY);
            return builder.build();
        }
        catch (IOException e) {
            throw new RuntimeException("counld not find Geo2  data file: ", e);
        }
    }

    @Description(value = "country or city lookup from ip")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice geoip(@SqlType(StandardTypes.VARCHAR) Slice category, @SqlType(StandardTypes.VARCHAR) Slice ip)
    {
        if (category == null || ip == null) {
            return null;
        }
        InetAddress ipAddress = null;
        String categoryStr = category.toStringUtf8();
        try {
            ipAddress = InetAddress.getByName(ip.toStringUtf8());
            switch (categoryStr.toLowerCase()) {
                case "country": {
                    return Slices.copiedBuffer(country.country(ipAddress).getCountry().getName(), charset);
                }
                case "city": {
                    CityResponse response = city.city(ipAddress);
                    return Slices.copiedBuffer((response.getCity().getName() == null ? "bi_null" : response.getCity().getName()), charset);
                }
                case "latitude": {
                    return Slices.copiedBuffer(String.valueOf(city.city(ipAddress).getLocation().getLatitude()), charset);
                }
                case "longitude": {
                    return Slices.copiedBuffer(String.valueOf(city.city(ipAddress).getLocation().getLongitude()), charset);
                }
                case "region": {
                    String region = "bi_null";
                    List<Subdivision> regions = city.city(ipAddress).getSubdivisions();
                    if (regions.size() != 0) {
                        region = regions.get(0).getName();
                    }
                    return Slices.copiedBuffer(region, charset);
                }
                default:
                    return null;
            }
        }
        catch (AddressNotFoundException addressNotFoundException) {
            addressNotFoundException.printStackTrace();
            return null;
        }
        catch (UnknownHostException unknownHostException) {
            unknownHostException.printStackTrace();
            return null;
        }
        catch (GeoIp2Exception geoIp2Exception) {
            geoIp2Exception.printStackTrace();
            return null;
        }
        catch (IOException ioException) {
            ioException.printStackTrace();
            return null;
        }
    }
}
