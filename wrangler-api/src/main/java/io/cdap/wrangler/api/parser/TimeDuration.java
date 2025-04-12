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
 * Represents a Token for TimeDuration, capable of parsing a duration string
 * (e.g., "5ms", "2.1s") and converting it to a canonical unit (milliseconds).
 */
@PublicEvolving
public class TimeDuration implements Token {

    // Multipliers to convert units to MILLISECONDS
    private static final double MILLIS_PER_NANOSECOND = 1.0 / 1_000_000.0;
    private static final double MILLIS_PER_MICROSECOND = 1.0 / 1_000.0;
    private static final double MILLIS_PER_SECOND = 1_000.0;
    private static final double MILLIS_PER_MINUTE = 60.0 * MILLIS_PER_SECOND;
    private static final double MILLIS_PER_HOUR = 60.0 * MILLIS_PER_MINUTE;
    private static final double MILLIS_PER_DAY = 24.0 * MILLIS_PER_HOUR;

    // Store final value in milliseconds as double for precision
    private final double value;

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
     * Parses the given duration string and converts it into milliseconds.
     * Handles integer and floating-point numbers.
     *
     * @param durationString The duration string to parse.
     * @return The duration in milliseconds.
     * @throws IllegalArgumentException If the duration string format or unit is invalid.
     */
    private double parseDuration(String durationString) {
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
                multiplier = MILLIS_PER_NANOSECOND;
            } else if (durationString.endsWith("us")) {
                numericPart = durationString.substring(0, durationString.length() - 2);
                multiplier = MILLIS_PER_MICROSECOND;
            } else if (durationString.endsWith("ms")) {
                numericPart = durationString.substring(0, durationString.length() - 2);
                multiplier = 1.0; // Already in milliseconds
            } else if (durationString.endsWith("s")) {
                numericPart = durationString.substring(0, durationString.length() - 1);
                multiplier = MILLIS_PER_SECOND;
            } else if (durationString.endsWith("min")) {
                numericPart = durationString.substring(0, durationString.length() - 3);
                multiplier = MILLIS_PER_MINUTE;
            } else if (durationString.endsWith("m")) {
                numericPart = durationString.substring(0, durationString.length() - 1);
                multiplier = MILLIS_PER_MINUTE;
            } else if (durationString.endsWith("h")) {
                numericPart = durationString.substring(0, durationString.length() - 1);
                multiplier = MILLIS_PER_HOUR;
            } else if (durationString.endsWith("d")) {
                numericPart = durationString.substring(0, durationString.length() - 1);
                multiplier = MILLIS_PER_DAY;
            } else {
                throw new IllegalArgumentException("Invalid time duration format or unsupported unit in string: " + durationString);
            }

            if (numericPart.isEmpty()) {
                throw new IllegalArgumentException("Missing numeric value in duration string: " + durationString);
            }

            double parsedValue = Double.parseDouble(numericPart);
            if (parsedValue < 0) {
                throw new IllegalArgumentException("Duration value cannot be negative: " + durationString);
            }
            // Calculate the value in milliseconds
            return parsedValue * multiplier;

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value in duration string: " + durationString, e);
        }
    }

    /**
     * Returns the duration in the specified TimeUnit.
     * Note: Conversion might lose precision due to intermediate long conversion.
     *
     * @param unit The TimeUnit to convert to.
     * @return The duration in the specified unit.
     */
    public long getDuration(TimeUnit unit) {
        // Convert internal milliseconds value to the target unit
        return unit.convert((long) this.value, TimeUnit.MILLISECONDS);
    }

    /**
     * Returns the duration in milliseconds.
     * This is the canonical value.
     *
     * @return The duration in milliseconds.
     */
    public double getValue() {
        return value;
    }

    @Override
    public Object value() {
        return value; // Return the canonical double value (milliseconds)
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", TokenType.TIME_DURATION.name());
        object.addProperty("value", value); // Store the canonical double value
        return object;
    }

    @Override
    public String toString() {
        // Provide a reasonable string representation
        return value + "ms";
    }
}