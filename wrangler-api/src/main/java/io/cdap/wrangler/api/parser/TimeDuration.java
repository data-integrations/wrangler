/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *  Copyright © 2023 Google LLC // Update copyright year/holder if needed
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.util.concurrent.TimeUnit;

/**
 * Represents a Token for time durations, capable of parsing strings
 * like "5ms", "2.1s", or "3h" and converting them into milliseconds.
 */
@PublicEvolving
public class TimeDuration implements Token {

    // Time unit conversion factors (all in terms of milliseconds)
    private static final double MILLIS_IN_NANOSECOND = 1.0 / 1_000_000.0;
    private static final double MILLIS_IN_MICROSECOND = 1.0 / 1_000.0;
    private static final double MILLIS_IN_SECOND = 1_000.0;
    private static final double MILLIS_IN_MINUTE = 60.0 * MILLIS_IN_SECOND;
    private static final double MILLIS_IN_HOUR = 60.0 * MILLIS_IN_MINUTE;
    private static final double MILLIS_IN_DAY = 24.0 * MILLIS_IN_HOUR;

    // Final parsed duration value (in milliseconds)
    private final double durationMillis;

    /**
     * Constructs a TimeDuration object by parsing the provided duration string.
     *
     * @param inputDuration A string representing duration (e.g., "5ms", "2.5h").
     * @throws IllegalArgumentException if the string format is invalid.
     */
    public TimeDuration(String inputDuration) {
        this.durationMillis = parseDuration(inputDuration);
    }

    /**
     * Parses a duration string and converts it to milliseconds.
     *
     * @param durationStr Input duration string to parse.
     * @return Duration value in milliseconds.
     * @throws IllegalArgumentException if format or value is invalid.
     */
    private double parseDuration(String durationStr) {
        if (durationStr == null || durationStr.trim().isEmpty()) {
            throw new IllegalArgumentException("Duration string must not be null or empty.");
        }

        durationStr = durationStr.trim().toLowerCase();
        String numericPart;
        double conversionFactor;

        try {
            if (durationStr.endsWith("ns")) {
                numericPart = durationStr.substring(0, durationStr.length() - 2);
                conversionFactor = MILLIS_IN_NANOSECOND;
            } else if (durationStr.endsWith("us")) {
                numericPart = durationStr.substring(0, durationStr.length() - 2);
                conversionFactor = MILLIS_IN_MICROSECOND;
            } else if (durationStr.endsWith("ms")) {
                numericPart = durationStr.substring(0, durationStr.length() - 2);
                conversionFactor = 1.0;
            } else if (durationStr.endsWith("s")) {
                numericPart = durationStr.substring(0, durationStr.length() - 1);
                conversionFactor = MILLIS_IN_SECOND;
            } else if (durationStr.endsWith("min")) {
                numericPart = durationStr.substring(0, durationStr.length() - 3);
                conversionFactor = MILLIS_IN_MINUTE;
            } else if (durationStr.endsWith("m")) {
                numericPart = durationStr.substring(0, durationStr.length() - 1);
                conversionFactor = MILLIS_IN_MINUTE;
            } else if (durationStr.endsWith("h")) {
                numericPart = durationStr.substring(0, durationStr.length() - 1);
                conversionFactor = MILLIS_IN_HOUR;
            } else if (durationStr.endsWith("d")) {
                numericPart = durationStr.substring(0, durationStr.length() - 1);
                conversionFactor = MILLIS_IN_DAY;
            } else {
                throw new IllegalArgumentException(
                        "Invalid time duration format or unsupported unit in string: " + durationStr);
            }

            if (numericPart.isEmpty()) {
                throw new IllegalArgumentException("Missing numeric value in duration string: " + durationStr);
            }

            double parsedNumber = Double.parseDouble(numericPart);
            if (parsedNumber < 0) {
                throw new IllegalArgumentException("Duration value cannot be negative: " + durationStr);
            }

            return parsedNumber * conversionFactor;

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value in duration string: " + durationStr, e);
        }
    }

    /**
     * Converts and returns the duration in the given {@link TimeUnit}.
     *
     * @param unit Time unit to convert to.
     * @return Converted duration in the specified unit (rounded).
     */
    public long getDuration(TimeUnit unit) {
        return unit.convert((long) this.durationMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * @return Canonical duration value in milliseconds.
     */
    public double getValue() {
        return durationMillis;
    }

    @Override
    public Object value() {
        return durationMillis;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", TokenType.TIME_DURATION.name());
        object.addProperty("value", durationMillis);
        return object;
    }

    @Override
    public String toString() {
        return durationMillis + "ms";
    }
}
