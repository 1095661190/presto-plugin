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

/**
 * Represents a country.
 *
 * @author Matt Tucker
 */
public class Country
{

    private final String code;
    private final String name;

    /**
     * Creates a new Country.
     *
     * @param code the country code.
     * @param name the country name.
     */
    public Country(String code, String name)
    {
        this.code = code;
        this.name = name;
    }

    /**
     * Returns the ISO two-letter country code of this country.
     *
     * @return the country code.
     */
    public String getCode()
    {
        return code;
    }

    /**
     * Returns the name of this country.
     *
     * @return the country name.
     */
    public String getName()
    {
        return name;
    }
}
