/*
 * Copyright © 2023 Cask Data, Inc.
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
import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The TimeDuration class represents a duration of time with support for various
 * units
 * (ns, us/µs, ms, s, m, h, d). It implements the Token interface for use in the
 * wrangler parser system.
 */
@PublicEvolving
public class TimeDuration implements Token {
    private static final Pattern TIME_PATTERN = Pattern.compile("^(\\d+(?:\\.\\d+)?)(ns|us|µs|ms|s|m|h|d)$",
            Pattern.CASE_INSENSITIVE);
    private final String value;
    private final long nanoseconds;

    /**
     * Creates a new TimeDuration instance from a string representation.
     *
     * @param value The string representation of the time duration (e.g., "1ms",
     *              "1.5s", "2m")
     * @throws IllegalArgumentException if the value is not in a valid format
     */
    public TimeDuration(String value) {
        this.value = value;
        this.nanoseconds = parseNanoseconds(value);
    }

    /**
     * Parses the string representation into nanoseconds.
     *
     * @param value The string to parse
     * @return The number of nanoseconds
     * @throws IllegalArgumentException if the value is not in a valid format
     */
    private long parseNanoseconds(String value) {
        Matcher matcher = TIME_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid time duration format: " + value);
        }

        double number = Double.parseDouble(matcher.group(1));
        String unit = matcher.group(2).toLowerCase();

        // Handle microseconds with both 'us' and 'µs'
        if (unit.equals("µs")) {
            unit = "us";
        }

        switch (unit) {
            case "ns":
                return (long) number;
            case "us":
                return (long) (number * 1000);
            case "ms":
                return (long) (number * 1000 * 1000);
            case "s":
                return (long) (number * 1000 * 1000 * 1000);
            case "m":
                return (long) (number * 60 * 1000 * 1000 * 1000);
            case "h":
                return (long) (number * 60 * 60 * 1000 * 1000 * 1000);
            case "d":
                return (long) (number * 24 * 60 * 60 * 1000 * 1000 * 1000);
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + unit);
        }
    }

    /**
     * Gets the number of nanoseconds represented by this TimeDuration.
     *
     * @return The number of nanoseconds
     */
    public long getNanoseconds() {
        return nanoseconds;
    }

    /**
     * Gets the duration in microseconds.
     *
     * @return the duration in microseconds
     */
    public double getMicroseconds() {
        return nanoseconds / 1000.0;
    }

    /**
     * Gets the duration in milliseconds.
     *
     * @return the duration in milliseconds
     */
    public double getMilliseconds() {
        return nanoseconds / (1000.0 * 1000.0);
    }

    /**
     * Gets the duration in seconds.
     *
     * @return the duration in seconds
     */
    public double getSeconds() {
        return nanoseconds / (1000.0 * 1000.0 * 1000.0);
    }

    /**
     * Gets the duration in minutes.
     *
     * @return the duration in minutes
     */
    public double getMinutes() {
        return nanoseconds / (1000.0 * 1000.0 * 1000.0 * 60.0);
    }

    /**
     * Gets the duration in hours.
     *
     * @return the duration in hours
     */
    public double getHours() {
        return nanoseconds / (1000.0 * 1000.0 * 1000.0 * 60.0 * 60.0);
    }

    /**
     * Gets the duration in days.
     *
     * @return the duration in days
     */
    public double getDays() {
        return nanoseconds / (1000.0 * 1000.0 * 1000.0 * 60.0 * 60.0 * 24.0);
    }

    @Override
    public String value() {
        return value;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", TokenType.TIME_DURATION.name());
        object.addProperty("value", value);
        return object;
    }

    @Override
    public String toString() {
        if (nanoseconds < 1000) {
            return String.format("%.2fns", nanoseconds);
        } else if (nanoseconds < 1000 * 1000) {
            return String.format("%.2fµs", getMicroseconds());
        } else if (nanoseconds < 1000 * 1000 * 1000) {
            return String.format("%.2fms", getMilliseconds());
        } else if (nanoseconds < 1000 * 1000 * 1000 * 60) {
            return String.format("%.2fs", getSeconds());
        } else if (nanoseconds < 1000 * 1000 * 1000 * 60 * 60) {
            return String.format("%.2fm", getMinutes());
        } else if (nanoseconds < 1000 * 1000 * 1000 * 60 * 60 * 24) {
            return String.format("%.2fh", getHours());
        } else {
            return String.format("%.2fd", getDays());
        }
    }
}
