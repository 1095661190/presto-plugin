package com.maxmind.geoip;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class IPV4
{

    private static final Pattern ipPattern_v4 = Pattern.compile("^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."
            + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
            + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
            + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$");

    public static boolean isValidIPV4(String ip)
    {
        return ipPattern_v4.matcher(ip).matches();
    }

}
