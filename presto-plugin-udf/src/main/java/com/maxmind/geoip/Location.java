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

package com.maxmind.geoip;

public class Location
{
    public String countryCode;
    public String countryName;
    public String region;
    public String city;
    public String postalCode;
    public float latitude;
    public float longitude;
    public int dma_code;
    public int area_code;
    public int metro_code;

    private final static double EARTH_DIAMETER = 2 * 6378.2;
    private final static double PI = 3.14159265;
    private final static double RAD_CONVERT = PI / 180;

    public double distance(Location loc)
    {
        double deltaLat, deltaLon;
        double temp;

        float lat1 = latitude;
        float lon1 = longitude;
        float lat2 = loc.latitude;
        float lon2 = loc.longitude;

        // convert degrees to radians
        lat1 *= RAD_CONVERT;
        lat2 *= RAD_CONVERT;

        // find the deltas
        deltaLat = lat2 - lat1;
        deltaLon = (lon2 - lon1) * RAD_CONVERT;

        // Find the great circle distance
        temp = Math.pow(Math.sin(deltaLat / 2), 2) + Math.cos(lat1)
                * Math.cos(lat2) * Math.pow(Math.sin(deltaLon / 2), 2);
        return EARTH_DIAMETER
                * Math.atan2(Math.sqrt(temp), Math.sqrt(1 - temp));
    }
}
