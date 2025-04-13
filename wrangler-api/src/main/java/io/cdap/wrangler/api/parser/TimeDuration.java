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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TimeDuration is a Token implementation that represents a time duration with a unit.
 * It can parse strings like "150ms", "2.1s", "5min", "1.5h", etc.
 */
public class TimeDuration implements Token {
    // Regex pattern to match a number followed by a time unit
    private static final Pattern PATTERN = Pattern.compile("^(\\d+(?:\\.\\d+)?)\\s*(ns|ms|s|min|h)$", Pattern.CASE_INSENSITIVE);

    // The numeric value
    private final double value;

    // The unit (ns, ms, s, min, h)
    private final String unit;

    // The number of nanoseconds (canonical unit)
    private final long nanoseconds;

    /**
     * Constructs a TimeDuration token by parsing the input string.
     *
     * @param value The string representation of the time duration (e.g. "150ms", "2.1s")
     */
    public TimeDuration(String value) {
        super();

        Matcher matcher = PATTERN.matcher(value.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    String.format("Invalid time duration format: '%s'. Expected format: number followed by ns, ms, s, min, or h", value));
        }

        this.value = Double.parseDouble(matcher.group(1));
        this.unit = matcher.group(2).toLowerCase();
        this.nanoseconds = calculateNanoseconds(this.value, this.unit);
    }

    /**
     * Calculates the number of nanoseconds based on the value and unit.
     *
     * @param value The numeric value
     * @param unit The unit (ns, ms, s, min, h)
     * @return The number of nanoseconds
     */
    private long calculateNanoseconds(double value, String unit) {
        switch (unit) {
            case "ns":
                return (long) value;
            case "ms":
                return (long) (value * 1_000_000);
            case "s":
                return (long) (value * 1_000_000_000);
            case "min":
                return (long) (value * 60 * 1_000_000_000);
            case "h":
                return (long) (value * 60 * 60 * 1_000_000_000);
            default:
                throw new IllegalArgumentException("Unknown time unit: " + unit);
        }
    }

    /**
     * Gets the raw numeric value.
     *
     * @return The numeric value without the unit
     */
    public double getValue() {
        return value;
    }

    /**
     * Gets the unit (ns, ms, s, min, h).
     *
     * @return The unit as a string
     */
    public String getUnit() {
        return unit;
    }

    /**
     * Gets the duration in nanoseconds.
     *
     * @return The number of nanoseconds
     */
    public long getNanoseconds() {
        return nanoseconds;
    }

    /**
     * Gets the duration in milliseconds.
     *
     * @return The number of milliseconds
     */
    public double getMilliseconds() {
        return nanoseconds / 1_000_000.0;
    }

    /**
     * Gets the duration in seconds.
     *
     * @return The number of seconds
     */
    public double getSeconds() {
        return nanoseconds / 1_000_000_000.0;
    }

    /**
     * Gets the duration in minutes.
     *
     * @return The number of minutes
     */
    public double getMinutes() {
        return nanoseconds / (60.0 * 1_000_000_000.0);
    }

    /**
     * Gets the duration in hours.
     *
     * @return The number of hours
     */
    public double getHours() {
        return nanoseconds / (60.0 * 60.0 * 1_000_000_000.0);
    }

    @Override
    public Object value() {
        return getNanoseconds();
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        return new com.google.gson.JsonPrimitive(getNanoseconds());
    }
}