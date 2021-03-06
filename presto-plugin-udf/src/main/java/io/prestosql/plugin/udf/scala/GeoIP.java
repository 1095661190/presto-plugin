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
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class GeoIP {

//    static Logger logger = LoggerFactory.getLogger(GeoIP.class);

    private static final Charset charset = StandardCharsets.UTF_8;
    private static final LookupService countryLookup;

    private static final LookupService cityLookup;

    private static final GeoIP2 geoip2 = new GeoIP2();

    private static final Set<String> v4_categorys = new HashSet<>(5);

    private GeoIP() {
    }

    static {
        v4_categorys.add("country");
        v4_categorys.add("city");
        v4_categorys.add("latitude");
        v4_categorys.add("longitude");
        v4_categorys.add("region");
        v4_categorys.add("province");
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
            InputStream inCountry = GeoIP.class.getClassLoader().getResourceAsStream("GeoIP.dat");
            byte[] buffer = new byte[8096];
            int offset = 0;
            while ((offset = inCountry.read(buffer, 0, buffer.length)) > 0) {
                country_file.write(buffer, 0, offset);
            }
            inCountry.close();
            InputStream inCity = GeoIP.class.getClassLoader().getResourceAsStream("GeoIPCity.dat");
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

    private LookupService createLookup(String dataFile) {
        try {
            return new LookupService(new File(dataFile));
        } catch (IOException e) {
            throw new RuntimeException("counld not find Geo data file: " + dataFile, e);
        }
    }

    private static LookupService createLookup(File dataFile) {
        try {
            return new LookupService(dataFile);
        } catch (IOException e) {
            throw new RuntimeException("counld not find Geo data file: " + dataFile, e);
        }
    }

    private static LookupService createLookup(RandomAccessFile dataFile) {
        try {
            return new LookupService(dataFile);
        } catch (IOException e) {
            throw new RuntimeException("counld not find Geo data file: " + dataFile, e);
        }
    }

    private static final Pattern ipPattern_v4 = Pattern.compile("^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."
            + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
            + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
            + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$");

    private static boolean isValidIPV4(String ip) {
        if (ip == null || "".equals(ip)) {
            return false;
        }
        return ipPattern_v4.matcher(ip).matches();
    }

    @Description(value = "country or city lookup from ip")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice geoip(@SqlType(StandardTypes.VARCHAR) Slice category, @SqlType(StandardTypes.VARCHAR) Slice ip) {
        String categoryStr = category.toStringUtf8();
        String ipStr = ip.toStringUtf8();
        if (category == null || ip == null) {
            return null;
        }

        try {
            if (isValidIPV4(ipStr) && v4_categorys.contains(categoryStr)) {
                if ("province".equals(category)) {
                    categoryStr = "region";
                }
                Location location;
                Location l0;
                Location l1;
                Location l11;
                String localRegion;
                switch (categoryStr.toLowerCase()) {
                    case "country":
                        return Slices.copiedBuffer(countryLookup.getCountry(ipStr).getName(), charset);
                    case "city":
                        location = cityLookup.getLocation(ipStr);
                        if (location == null) {
                            return Slices.copiedBuffer( "bi_null", charset);
                        }
                        return Slices.copiedBuffer(  (location.city == null) ? "bi_null" : location.city, charset);
                    case "latitude":
                        l0 = cityLookup.getLocation(ipStr);
                        return Slices.copiedBuffer((String) (l0 == null ? "" + -1 : "" + l0.latitude), charset);
                    case "longitude":
                        l1 = cityLookup.getLocation(ipStr);
                        return Slices.copiedBuffer((String) (l1 == null ? "" + -1 : "" + l1.longitude), charset);
                    case "region":
                        l11 = cityLookup.getLocation(ipStr);
                        if (l11 == null) {
                            return Slices.copiedBuffer( "bi_null", charset);
                        }
                        localRegion = regionName.regionNameByCode(l11.countryCode, l11.region);
                        return Slices.copiedBuffer( (localRegion == null) ? "bi_null" : localRegion, charset);
                    default:
                        return Slices.copiedBuffer( "bi_null", charset);
                }
            } else {
                return Slices.copiedBuffer(geoip2.evaluate(categoryStr, ipStr), charset);
            }

        } catch (ArrayIndexOutOfBoundsException e) {
            return null;
        }
    }
}
