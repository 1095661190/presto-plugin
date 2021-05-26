package io.prestosql.plugin.udf.scala;

import com.alibaba.fastjson.JSONObject;
import io.airlift.slice.Slice;
import io.prestosql.plugin.udf.util.IPV4Parser;
import io.prestosql.plugin.udf.util.Tools;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;


public class IPV4 {

    private static IPV4Parser parser;
    public IPV4(){
        parser = IPV4Parser.getInstance();
    }

    private final static String BI_NULL = "bi_null";





    @Description(value = "")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static String alip(@SqlType(StandardTypes.VARCHAR) Slice category_str,@SqlType(StandardTypes.VARCHAR) Slice ip_str) {

        String category = category_str.toStringUtf8();
        String ip = ip_str.toStringUtf8();

        boolean isIP = Tools.isValidIP(ip);
        if (category == null || !isIP) {
            return BI_NULL;
        }
        String location = null;
        try {
            location = parser.findipv4(ip);
        } catch (Exception e) {
            e.printStackTrace();
            return BI_NULL;
        }
        if (location == null){
            return BI_NULL;
        }

        JSONObject jsonObject_rtn = null;
        try {
            jsonObject_rtn = JSONObject.parseObject(location);
        } catch (Exception e) {
            e.printStackTrace();
            return BI_NULL;
        }
        switch (category.toLowerCase()) {
            case "country":
                return getValuefromJson("country", jsonObject_rtn);
            case "province":
                return getValuefromJson("province", jsonObject_rtn);
            case "city":
                return getValuefromJson("city", jsonObject_rtn);
            case "operator":
                return getValuefromJson("isp", jsonObject_rtn);
            case "country_code":
                return getValuefromJson("country_code", jsonObject_rtn);
            case "country_en":
                return getValuefromJson("country_en", jsonObject_rtn);
            case "province_code":
                return getValuefromJson("province_code", jsonObject_rtn);
            case "province_en":
                return getValuefromJson("province_en", jsonObject_rtn);
            case "city_code":
                return getValuefromJson("city_code", jsonObject_rtn);
            case "city_en":
                return getValuefromJson("city_en", jsonObject_rtn);
            case "county":
                return getValuefromJson("county", jsonObject_rtn);
            case "county_code":
                return getValuefromJson("county_code", jsonObject_rtn);
            case "isp_code":
                return getValuefromJson("isp_code", jsonObject_rtn);
            case "routes":
                return getValuefromJson("routes", jsonObject_rtn);
            case "longitude":
                return getValuefromJson("longitude", jsonObject_rtn);
            case "latitude":
                return getValuefromJson("latitude", jsonObject_rtn);
            default:
                return BI_NULL;
        }
    }



    private static  String getValuefromJson(String name, JSONObject json) {
        if (json == null || json.size() == 0) {
            return BI_NULL;
        }
        if (!json.containsKey(name)) {
            return BI_NULL;
        }
        Object o = json.get(name);
        if (o != null && Tools.isNotEmpty(o.toString())) {
            return o.toString();
        }
        return BI_NULL;
    }




}
