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
 * Signals that there was an issue reading from the database file due to
 * unexpected data formatting. This generally suggests that the database is
 * corrupt or otherwise not in a format supported by the reader.
 */
public final class InvalidDatabaseException
        extends RuntimeException
{
    /**
     * @param message A message describing the reason why the exception was thrown.
     */
    public InvalidDatabaseException(String message)
    {
        super(message);
    }

    /**
     * @param message A message describing the reason why the exception was thrown.
     * @param cause The cause of the exception.
     */
    public InvalidDatabaseException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
