/*
 * Copyright © 2024 Cask Data, Inc.
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
 * Represents a time duration value with support for different units (ms, s, m, h).
 * This class parses string representations of time durations and provides methods to access the value in milliseconds.
 */
public class TimeDuration implements Token {
    private final String original;
    private final long milliseconds;

    /**
     * Creates a new TimeDuration instance from a string token.
     *
     * @param token the string token representing a time duration (e.g., "1h", "30m", "45s")
     */
    public TimeDuration(String token) {
        this.original = token;
        this.milliseconds = parseDuration(token);
    }

    /**
     * Parses a string representation of a time duration into milliseconds.
     *
     * @param input the string to parse
     * @return the duration in milliseconds
     * @throws IllegalArgumentException if the input format is invalid
     */
    private long parseDuration(String input) {
        input = input.trim().toLowerCase();
        if (input.endsWith("ms")) {
            return Long.parseLong(input.replace("ms", ""));
        }
        if (input.endsWith("s")) {
            return Long.parseLong(input.replace("s", "")) * 1000;
        }
        if (input.endsWith("m")) {
            return Long.parseLong(input.replace("m", "")) * 60 * 1000;
        }
        if (input.endsWith("h")) {
            return Long.parseLong(input.replace("h", "")) * 60 * 60 * 1000;
        }
        throw new IllegalArgumentException("Invalid time duration format: " + input);
    }

    @Override
    public Object value() {
        return milliseconds;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    /**
     * Gets the time duration in milliseconds.
     *
     * @return the time duration in milliseconds
     */
    public long getMilliseconds() {
        return milliseconds;
    }

    /**
     * Returns the duration in milliseconds.
     *
     * @return the duration in milliseconds
     */
    public long getMillis() {
        return parseDuration(original);
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(milliseconds);
    }

    /**
     * Returns the duration in milliseconds.
     *
     * @return the duration in milliseconds
     */
    public long toMillis() {
        return milliseconds;
    }

    /**
     * Format the time duration to a specified unit.
     *
     * @param unit The target unit (ms, s, m, h)
     * @return The formatted string with the specified unit
     * @throws IllegalArgumentException if the unit is not one of: ms, s, m, h
     */
    public String format(String unit) {
        double value = milliseconds;
        switch (unit.toLowerCase()) {
            case "ms":
                return String.format("%.0fms", value);
            case "s":
                return String.format("%.2fs", value / 1000);
            case "m":
                return String.format("%.2fm", value / (60 * 1000));
            case "h":
                return String.format("%.2fh", value / (60 * 60 * 1000));
            default:
                throw new IllegalArgumentException("Invalid unit: " + unit + ". Must be one of: ms, s, m, h");
        }
    }
}
