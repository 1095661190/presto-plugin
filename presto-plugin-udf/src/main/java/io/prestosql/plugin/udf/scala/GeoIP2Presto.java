package io.prestosql.plugin.udf.scala;

import com.maxmind.db.Reader;
import com.maxmind.geoip.IPV4;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class GeoIP2Presto
{
    private static final Charset charset = StandardCharsets.UTF_8;

    /**
     *GeoLite2-City 数据库文件
     */
    private static final String CITY_DATA_FILE = "GeoLite2-City.mmdb";
    private static final String COUNTRY_DATA_FILE = "GeoIP2-Country.mmdb";

    /**
     * load GeoIP2-Country from  local file
     */
//    private static final DatabaseReader country = createDatabaseReader(new File("/usr/share/GeoIP/" + COUNTRY_DATA_FILE));
//    private static final DatabaseReader city = createDatabaseReader(new File("/usr/share/GeoIP/" + CITY_DATA_FILE));


    private static final DatabaseReader country =  createDatabaseReader(GeoIP2Presto.class.getClassLoader().getResourceAsStream(COUNTRY_DATA_FILE));
    private static final DatabaseReader city = createDatabaseReader(GeoIP2Presto.class.getClassLoader().getResourceAsStream(CITY_DATA_FILE));

    /**
     *
     * @param file
     * @return
     */
    private static DatabaseReader createDatabaseReader(File file)
    {
        try {
            DatabaseReader.Builder builder = new DatabaseReader.Builder(file);
            builder.fileMode(Reader.FileMode.MEMORY);
            return builder.build();
        }
        catch (IOException e) {
            throw new RuntimeException("counld not find Geo2  data file: ", e);
        }
    }

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


    /*@Description(value = "country or city lookup from ip")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)*/
    public static Slice geoip(@SqlType(StandardTypes.VARCHAR) Slice category, @SqlType(StandardTypes.VARCHAR) Slice ip)
    {
        if (category == null || ip == null) {
            return null;
        }
        if(IPV4.isValidIPV4(ip.toStringUtf8())){
            return IpLookup.geoip(category,ip);
        }

        InetAddress ipAddress = null;
        String categoryStr = category.toStringUtf8();
        try {
            ipAddress = InetAddress.getByName(ip.toStringUtf8());
            switch (categoryStr.toLowerCase()) {
                case "country": {
                    CountryResponse response = country.country(ipAddress);
                    String code = response.getCountry().getIsoCode();
                    //ipv6  优先使用 old country name
                    String old_name = LookupService.countryMap.get(code);
                    return Slices.copiedBuffer(old_name == null ? response.getCountry().getName() : old_name, charset);
                }
              /*  case "city": {
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
                }*/
                default:
                    return Slices.copiedBuffer("bi_null", charset);
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

