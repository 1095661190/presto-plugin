package io.prestosql.plugin.udf.scala;

import com.maxmind.db.Reader;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class GeoIP2_2_bak {

    //    private static final String CITY_DATA_FILE = "GeoIP2-City.mmdb";
    private static final String COUNTRY_DATA_FILE = "GeoIP2-Country.mmdb";
    //    private final DatabaseReader databaseReader_country = createCountryDatabaseReader(new File("/Users/happyelements/usr/share/GeoIP/" + COUNTRY_DATA_FILE));
//    private final DatabaseReader databaseReader_country = createCountryDatabaseReader(new File("/usr/share/GeoIP/" + COUNTRY_DATA_FILE));
    private final DatabaseReader databaseReader_country = createCountryDatabaseReader(GeoIP2.class.getClassLoader().getResourceAsStream(COUNTRY_DATA_FILE));
//    private final DatabaseReader databaseReader_city = createCityDatabaseReader(GeoIP2.class.getClassLoader().getResourceAsStream(CITY_DATA_FILE));
//    private final DatabaseReader databaseReader_city = createCityDatabaseReader(new File("/Users/happyelements/usr/share/GeoIP/" + CITY_DATA_FILE));
//    private final DatabaseReader databaseReader_city = createCityDatabaseReader(new File("/usr/share/GeoIP/" + CITY_DATA_FILE));

    /**
     * @param dataFileIn #File/InputStream dataFileIn
     * @return
     */
    private DatabaseReader createCountryDatabaseReader(InputStream dataFileIn) {
        try {
            DatabaseReader.Builder builder = new DatabaseReader.Builder(dataFileIn);
            builder.fileMode(Reader.FileMode.MEMORY);
            return builder.build();
        } catch (IOException e) {
            throw new RuntimeException("counld not find Geo2 country data file: ", e);
        }
    }
//
//    private DatabaseReader createCityDatabaseReader(File dataFileIn) {
//        try {
//            DatabaseReader.Builder builder = new DatabaseReader.Builder(dataFileIn);
//            builder.fileMode(Reader.FileMode.MEMORY);
//            return builder.build();
//        } catch (IOException e) {
//            throw new RuntimeException("counld not find Geo2 city data file: ", e);
//        }
//    }

    public String evaluate(String category, String ip) {

        if (category == null || "".equals(category)) {
            return null;
        }
        try {
            InetAddress ipAddress = InetAddress.getByName(ip);
            if (category.contains("country")) {
                CountryResponse countryResponse = this.databaseReader_country.country(ipAddress);
                if (countryResponse == null) {
                    return "bi_null";
                } else if (category.equals("country")) {
                    String country = countryResponse.getCountry().getName();
                    String country_code = countryResponse.getCountry().getIsoCode();
                    if (country != null && country_code != null) {
                        if (LookupService.countryMap.containsKey(country_code)) {
                            return LookupService.countryMap.get(country_code);
                        } else {
                            return country;
                        }
                    }
                } else if (category.equals("country_name")) {
                    String country_name = countryResponse.getCountry().getNames().get("zh-CN");
                        return country_name;
                } else if (category.equals("country_code")) {
                    String country_code = countryResponse.getCountry().getIsoCode();
                        return country_code;
                }
//                else if (StringUtils.equals("country_isInEuropeanUnion", category)) {
//                    boolean is_eurp = countryResponse.getCountry().isInEuropeanUnion();
//                    if (is_eurp) return "true";
//                    else return "false";
//                }
            } else if (category.contains("continent")) {
                CountryResponse countryResponse = this.databaseReader_country.country(ipAddress);
                if (countryResponse == null) {
                    return "bi_null";
                } else if (category.equals("continent")) {
                    String continent = countryResponse.getContinent().getName();
                    if (continent!=null) {
                        return continent;
                    }
                } else if (category.equals("continent_code")) {
                    String continent_code = countryResponse.getContinent().getCode();
                        return continent_code;
                } else if (category.equals("continent_name")) {
                    String continent_name = countryResponse.getContinent().getNames().get("zh-CN");
                        return continent_name;
                }
            } else if (category.contains("all")) {
                CountryResponse countryResponse = this.databaseReader_country.country(ipAddress);
                if (countryResponse == null) {
                    return "bi_null";
                } else {
                    String all_rtn_json = countryResponse.toJson();
                        return all_rtn_json;

                }

            } else {
//                CityResponse cityResponse = this.databaseReader_city.city(ipAddress);
//                if (StringUtils.equals("timeZone", category)) {
//                    String timeZone = cityResponse.getLocation().getTimeZone();
//                    if (StringUtils.isNotBlank(timeZone)) return timeZone;
//                } else if (StringUtils.equals("latitude", category)) {
//                    String latitude = cityResponse.getLocation().getLatitude().toString();
//                    if (StringUtils.isNotBlank(latitude)) return latitude;
//                } else if (StringUtils.equals("longitude", category)) {
//                    String longitude = cityResponse.getLocation().getLongitude().toString();
//                    if (StringUtils.isNotBlank(longitude)) return longitude;
//                } else if (StringUtils.equals("province", category)) {
//                    if (cityResponse.getSubdivisions() == null || cityResponse.getSubdivisions().size() == 0) {
//                        return null;
//                    }
//                    String province = cityResponse.getSubdivisions().get(0).getName();
//                    if (StringUtils.isNotBlank(province)) return province;
//                } else if (StringUtils.equals("province_code", category)) {
//                    if (cityResponse.getSubdivisions() == null || cityResponse.getSubdivisions().size() == 0) {
//                        return null;
//                    }
//                    String province_code = cityResponse.getSubdivisions().get(0).getIsoCode();
//                    if (StringUtils.isNotBlank(province_code)) return province_code;
//                } else if (StringUtils.equals("province_name", category)) {
//                    if (cityResponse.getSubdivisions() == null || cityResponse.getSubdivisions().size() == 0) {
//                        return null;
//                    }
//                    String province_name = cityResponse.getSubdivisions().get(0).getNames().get("zh-CN");
//                    if (StringUtils.isNotBlank(province_name)) return province_name;
//                } else if (StringUtils.equals("postal", category)) {
//                    String postal = cityResponse.getPostal().getCode();
//                    if (StringUtils.isNotBlank(postal)) return postal;
//                } else if (StringUtils.equals("city", category)) {
//                    String city = cityResponse.getCity().getName();
//                    if (StringUtils.isNotBlank(city)) return city;
//                } else if (StringUtils.equals("city_name", category)) {
//                    String city_name = cityResponse.getCity().getNames().get("zh-CN");
//                    if (StringUtils.isNotBlank(city_name)) return city_name;
//                }
            }
        } catch (AddressNotFoundException addressNotFoundException) {
            addressNotFoundException.printStackTrace();
            return null;
        } catch (UnknownHostException unknownHostException) {
            unknownHostException.printStackTrace();
            return null;
        } catch (GeoIp2Exception geoIp2Exception) {
            geoIp2Exception.printStackTrace();
            return null;
        } catch (IOException ioException) {
            ioException.printStackTrace();
            return null;
        }
        return null;
    }
}
