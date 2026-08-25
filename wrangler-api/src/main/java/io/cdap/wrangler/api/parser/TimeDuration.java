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
import com.google.gson.JsonObject;

import java.util.Locale;

/**
 * Represents a token for time duration values with units (e.g., ms, s, m, h).
 */
public class TimeDuration implements Token {

    private Double time;

    /**
     * Constructs a TimeDuration token.
     *
     * @param value The string representation of time value (e.g., "10sec").
     */
    public TimeDuration(String value) {
        this.time = parseMilliSeconds(value);
    }

    /**
     * Returns the value of this {@code TIME_DURATION} object as a
     * {@code Double} representing the time in milliseconds.
     *
     * @return the time (milliseconds) value of this object.
     */
    @Override
    public Double value() {
        return getTime();
    }

    /**
     * Returns the value in milliseconds.
     *
     * @return time value in double.
     */
    public double getTime() {
        return this.time;
    }

    /**
     * Returns the type of this {@code TIME_DURATION} object as a
     * {@code TokenType} enum.
     *
     * @return the enumerated {@code TokenType} of this object.
     */
    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    /**
     * Parses the input string and converts it to milliseconds.
     *
     * @param value The input time duration string.
     * @return The time in milliseconds.
     * @throws IllegalArgumentException if input is not a valid time duration
     * string.
     */
    private Double parseMilliSeconds(String value) {
        String normalize = value.trim().toLowerCase(Locale.ENGLISH);
        double valueDouble = Double.parseDouble(normalize.replaceAll("[^0-9.\\-]", ""));
        String unit = normalize.replaceAll("[^a-zA-Z]+", "");
        if (valueDouble < 0) {
            throw new IllegalArgumentException("Negative time values are not allowed: " + value);
        }

        if (unit.equals("ms")) {
            return valueDouble;
        } else if (unit.equals("s") 
                || unit.equals("sec") 
                || unit.equals("secs")) {
            return valueDouble * 1000;
        } else if (unit.equals("m") || unit.equals("min")) {
            return valueDouble * 1000 * 60;
        } else if (unit.equals("hours") || normalize.contains("hours")) { // Adjusted condition
            System.out.println(valueDouble + " valueDouble");
            return valueDouble * 1000 * 60 * 60;
        } else if (unit.equals("hour")) {
            return valueDouble * 1000 * 60 * 60;
        } else if (unit.equals("hr")) {
            return valueDouble * 1000 * 60 * 60;
        } else if (unit.equals("h")) {
            return valueDouble * 1000 * 60 * 60;
        } else {
            throw new IllegalArgumentException("Unsupported time unit in: " + value);
        }
    }

    /**
     * Returns the members of this {@code TIME_DURATION} object as a
     * {@code JsonElement}.
     *
     * @return Json representation of this {@code TIME_DURATION} object as
     * {@code JsonElement}
     */
    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", TokenType.TIME_DURATION.name());
        object.addProperty("value", this.time);
        return object;
    }
}
