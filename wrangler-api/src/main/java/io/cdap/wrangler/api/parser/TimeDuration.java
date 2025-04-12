/*
 *  Copyright © 2017-2019 Cask Data, Inc.
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
 * Represents a Token for TimeDuration, capable of parsing a duration string
 * (e.g., "5ms", "2.1s") and converting it to a canonical unit (nanoseconds).
 */
@PublicEvolving
public class TimeDuration implements Token {

    // Multipliers to convert units to nanoseconds (use double for accuracy with fractions)
    private static final double NANOS_PER_MICROSECOND = 1_000.0;
    private static final double NANOS_PER_MILLISECOND = 1_000_000.0;
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;
    private static final double NANOS_PER_MINUTE = 60.0 * NANOS_PER_SECOND;
    private static final double NANOS_PER_HOUR = 60.0 * NANOS_PER_MINUTE;
    private static final double NANOS_PER_DAY = 24.0 * NANOS_PER_HOUR;

    private final long value; // Store final value in nanoseconds as long

    /**
     * Constructs a TimeDuration by parsing the given duration string.
     *
     * @param value The duration string to parse (e.g., "5ms", "2.1s").
     * @throws IllegalArgumentException If the duration string is invalid.
     */
    public TimeDuration(String value) {
        this.value = parseDuration(value);
    }

    /**
     * Parses the given duration string and converts it into nanoseconds.
     * Handles integer and floating-point numbers.
     *
     * @param durationString The duration string to parse.
     * @return The duration in nanoseconds (truncated to long).
     * @throws IllegalArgumentException If the duration string format or unit is invalid.
     */
    private long parseDuration(String durationString) {
        if (durationString == null || durationString.trim().isEmpty()) {
            throw new IllegalArgumentException("Duration string must not be null or empty.");
        }

        // Use lowercase for units consistency
        durationString = durationString.trim().toLowerCase();
        String numericPart;
        double multiplier;

        try {
            if (durationString.endsWith("ns")) {
                numericPart = durationString.substring(0, durationString.length() - 2);
                multiplier = 1.0;
            } else if (durationString.endsWith("us")) {
                numericPart = durationString.substring(0, durationString.length() - 2);
                multiplier = NANOS_PER_MICROSECOND;
            } else if (durationString.endsWith("ms")) {
                numericPart = durationString.substring(0, durationString.length() - 2);
                multiplier = NANOS_PER_MILLISECOND;
            } else if (durationString.endsWith("s")) {
                numericPart = durationString.substring(0, durationString.length() - 1);
                multiplier = NANOS_PER_SECOND;
            } else if (durationString.endsWith("min")) { // Support "min" as requested by test
                numericPart = durationString.substring(0, durationString.length() - 3);
                multiplier = NANOS_PER_MINUTE;
            } else if (durationString.endsWith("m")) { // Also support "m" for minutes
                numericPart = durationString.substring(0, durationString.length() - 1);
                multiplier = NANOS_PER_MINUTE;
            } else if (durationString.endsWith("h")) {
                numericPart = durationString.substring(0, durationString.length() - 1);
                multiplier = NANOS_PER_HOUR;
            } else if (durationString.endsWith("d")) {
                numericPart = durationString.substring(0, durationString.length() - 1);
                multiplier = NANOS_PER_DAY;
            } else {
                // Match the test's expected generic message format
                throw new IllegalArgumentException("Invalid time duration format or unsupported unit in string: " + durationString);
            }

            if (numericPart.isEmpty()) {
                throw new IllegalArgumentException("Missing numeric value in duration string: " + durationString);
            }

            double parsedValue = Double.parseDouble(numericPart);
            if (parsedValue < 0) {
                throw new IllegalArgumentException("Duration value cannot be negative: " + durationString);
            }
            // Cast to long truncates fractional nanoseconds.
            return (long) (parsedValue * multiplier);

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value in duration string: " + durationString, e);
        }
    }

    /**
     * Returns the duration in the specified TimeUnit.
     * Note: Conversion might lose precision due to integer division.
     *
     * @param unit The TimeUnit to convert to.
     * @return The duration in the specified unit.
     */
    public long getDuration(TimeUnit unit) {
        return unit.convert(this.value, TimeUnit.NANOSECONDS);
    }

    /**
     * Returns the duration in nanoseconds.
     * This is the canonical value.
     *
     * @return The duration in nanoseconds.
     */
    public long getValue() {
        return value;
    }

    @Override
    public Object value() {
        return value; // Return the canonical long value (nanoseconds)
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", TokenType.TIME_DURATION.name());
        object.addProperty("value", value); // Store the canonical long value
        return object;
    }

    @Override
    public String toString() {
        // Provide a reasonable string representation, maybe the original input if stored,
        // or reconstruct. For simplicity, just return nanoseconds.
        return value + "ns";
    }
}