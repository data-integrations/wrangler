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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a TimeDuration token used in parsing directives.
 * Accepts strings like "10s", "5m", "2h", "100ms", "3d", "1w", "500us",
 * "1000ns" and converts them to milliseconds.
 */
public class TimeDuration implements Token {

    private static final Pattern DURATION_PATTERN = Pattern.compile("^(\\d+)\\s*(ns|us|ms|s|m|h|d|w)$", 
                                                  Pattern.CASE_INSENSITIVE);
    // Stores the duration in milliseconds after parsing.
    private final long milliseconds;

    /**
     * Constructs the TimeDuration token by parsing the input.
     *
     * @param input The time duration string (e.g., "10s", "100ms").
     * @throws IllegalArgumentException if the input format is invalid
     */
    public TimeDuration(String input) {
        this.milliseconds = parseDuration(input);
    }

    /**
     * Parses a human-readable time duration string into milliseconds.
     * Only whole-number durations are supported.
     *
     * Supported formats:
     * - "ns" for nanoseconds
     * - "us" for microseconds
     * - "ms" for milliseconds
     * - "s" for seconds
     * - "m" for minutes
     * - "h" for hours
     * - "d" for days
     * - "w" for weeks
     *
     * @param input The input string (e.g., "10s", "5m").
     * @return The duration in milliseconds as a long.
     * @throws IllegalArgumentException if the unit is unrecognized or format is invalid.
     */
    private long parseDuration(String input) {
        if (input == null) {
            throw new IllegalArgumentException("Input cannot be null");
        }
        
        input = input.trim().toLowerCase();
        Matcher matcher = DURATION_PATTERN.matcher(input);
        
        if (matcher.matches()) {
            long value = Long.parseLong(matcher.group(1));
            String unit = matcher.group(2);
            
            switch (unit) {
                case "ns":
                    return value / 1_000_000L;
                case "us":
                    return value / 1_000L;
                case "ms":
                    return value;
                case "s":
                    return value * 1000L;
                case "m":
                    return value * 60L * 1000L;
                case "h":
                    return value * 60L * 60L * 1000L;
                case "d":
                    return value * 24L * 60L * 60L * 1000L;
                case "w":
                    return value * 7L * 24L * 60L * 60L * 1000L;
                default:
                    // Should never happen due to regex pattern
                    throw new IllegalArgumentException("Unsupported time unit: " + unit);
            }
        }
        
        throw new IllegalArgumentException("Invalid time duration format: " + input);
    }

    /**
     * Converts milliseconds to the specified unit.
     *
     * @param millis The milliseconds.
     * @param unit   The target unit (e.g., "s", "m").
     * @return The converted value as a long.
     * @throws IllegalArgumentException if the unit is not supported.
     */
    public static long convertMillisecondsToUnit(long millis, String unit) {
        if (unit == null) {
            throw new IllegalArgumentException("Unit cannot be null");
        }
        
        switch (unit.toLowerCase()) {
            case "ns":
                return millis * 1_000_000L;
            case "us":
                return millis * 1_000L;
            case "ms":
                return millis;
            case "s":
                return millis / 1000L;
            case "m":
                return millis / (1000L * 60L);
            case "h":
                return millis / (1000L * 60L * 60L);
            case "d":
                return millis / (1000L * 60L * 60L * 24L);
            case "w":
                return millis / (1000L * 60L * 60L * 24L * 7L);
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + unit);
        }
    }

    /**
     * Returns the duration in milliseconds.
     *
     * @return The duration in milliseconds.
     */
    public long getMilliseconds() {
        return milliseconds;
    }

    @Override
    public Object value() {
        return milliseconds;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(milliseconds);
    }
}
