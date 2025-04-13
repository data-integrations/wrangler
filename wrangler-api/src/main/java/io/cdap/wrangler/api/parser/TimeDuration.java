/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

/**
 * The TimeDuration class parses a token string representing a time duration (e.g., "150ms")
 * and provides a method to retrieve its value in milliseconds.
 */
public class TimeDuration implements Token {
    private final double value;
    private final String unit;

    /**
     * Constructs a TimeDuration token by parsing the given token string.
     *
     * @param token the token string, for example "150ms" or "2s"
     */
    public TimeDuration(String token) {
        int pos = 0;
        while (pos < token.length() &&
               (Character.isDigit(token.charAt(pos)) || token.charAt(pos) == '.')) {
            pos++;
        }
        this.value = Double.parseDouble(token.substring(0, pos));
        this.unit = token.substring(pos).toLowerCase();
    }

    /**
     * Converts the parsed time duration into milliseconds.
     *
     * @return the time duration in milliseconds.
     */
    public long getMilliseconds() {
        switch (unit) {
            case "ms":
                return (long) value;
            case "s":
                return (long) (value * 1000);
            case "m":
                return (long) (value * 60 * 1000);
            case "h":
                return (long) (value * 3600 * 1000);
            default:
                throw new IllegalArgumentException("Unknown time unit: " + unit);
        }
    }

    /**
     * Returns the value of this token. Here, we use the computed milliseconds.
     */
    @Override
    public Object value() {
        return getMilliseconds();
    }

    /**
     * Returns the type of this token.
     *
     * @return TokenType.TIME_DURATION
     */
    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    /**
     * Returns a JSON representation of this token.
     *
     * @return JsonElement representing this token.
     */
    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(getMilliseconds());
    }
}