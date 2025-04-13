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

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class <code>TimeDuration</code> represents the time duration token as identified by the directive parser.
 * It handles parsing and conversion of time duration strings like "150ms", "2.5s", "1min", etc.
 */
public class TimeDuration implements Token {
    // Regex pattern to match time duration strings
    private static final Pattern TIME_DURATION_PATTERN = Pattern.compile("^(\\d+(?:\\.\\d+)?)\\s*(ns|us|ms|s|min|h)$");

    // Unit conversion factors (to nanoseconds)
    private static final Map<String, Long> UNIT_MULTIPLIERS = new HashMap<>();
    static {
        UNIT_MULTIPLIERS.put("ns", 1L);                         // nanoseconds
        UNIT_MULTIPLIERS.put("us", 1000L);                      // microseconds
        UNIT_MULTIPLIERS.put("ms", 1000L * 1000L);              // milliseconds
        UNIT_MULTIPLIERS.put("s", 1000L * 1000L * 1000L);       // seconds
        UNIT_MULTIPLIERS.put("min", 60L * 1000L * 1000L * 1000L);     // minutes
        UNIT_MULTIPLIERS.put("h", 60L * 60L * 1000L * 1000L * 1000L); // hours
    }

    private final String original;   // The original time duration string
    private final double value;      // The parsed numeric value
    private final String unit;       // The unit (ns, us, ms, s, min, h)
    private final long nanoseconds;  // The canonical value in nanoseconds

    /**
     * Constructor for the TimeDuration token.
     *
     * @param durationStr The time duration string to parse (e.g. "150ms", "2.5s")
     * @throws IllegalArgumentException If the string is not a valid time duration format
     */
    public TimeDuration(String durationStr) {
        this.original = durationStr;

        Matcher matcher = TIME_DURATION_PATTERN.matcher(durationStr.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    String.format("Invalid time duration format: '%s'. Expected format: <number><unit> (e.g. 150ms, 2.5s)", durationStr));
        }

        this.value = Double.parseDouble(matcher.group(1));
        this.unit = matcher.group(2);

        if (!UNIT_MULTIPLIERS.containsKey(this.unit)) {
            throw new IllegalArgumentException(
                    String.format("Invalid time duration unit: '%s'. Supported units: ns, us, ms, s, min, h", this.unit));
        }

        this.nanoseconds = Math.round(this.value * UNIT_MULTIPLIERS.get(this.unit));
    }

    /**
     * Gets the original time duration string.
     *
     * @return The original time duration string
     */
    public String getOriginal() {
        return original;
    }

    /**
     * Gets the numeric value part of the time duration.
     *
     * @return The numeric value
     */
    public double getValue() {
        return value;
    }

    /**
     * Gets the unit part of the time duration.
     *
     * @return The unit (ns, us, ms, s, min, h)
     */
    public String getUnit() {
        return unit;
    }

    /**
     * Gets the canonical value in nanoseconds.
     *
     * @return The value in nanoseconds
     */
    public long getNanoseconds() {
        return nanoseconds;
    }

    /**
     * Converts the nanosecond value to a specified unit.
     *
     * @param targetUnit The target unit (ns, us, ms, s, min, h)
     * @return The value in the target unit
     * @throws IllegalArgumentException If the target unit is not supported
     */
    public double toUnit(String targetUnit) {
        if (!UNIT_MULTIPLIERS.containsKey(targetUnit)) {
            throw new IllegalArgumentException(
                    String.format("Invalid target unit: '%s'. Supported units: ns, us, ms, s, min, h", targetUnit));
        }

        return (double) nanoseconds / UNIT_MULTIPLIERS.get(targetUnit);
    }

    /**
     * Formats the time duration to a readable string in the specified unit.
     *
     * @param targetUnit The target unit (ns, us, ms, s, min, h)
     * @param precision The number of decimal places to include
     * @return A formatted string representation
     * @throws IllegalArgumentException If the target unit is not supported
     */
    public String format(String targetUnit, int precision) {
        double converted = toUnit(targetUnit);

        // Format with specified precision
        BigDecimal bd = BigDecimal.valueOf(converted);
        bd = bd.setScale(precision, BigDecimal.ROUND_HALF_UP);

        return bd.doubleValue() + " " + targetUnit;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public Object value() {
        return getNanoseconds();
    }

    @Override
    public JsonElement toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("nanoseconds", nanoseconds);
        json.addProperty("value", value);
        json.addProperty("unit", unit);
        json.addProperty("original", original);
        return json;
    }
}