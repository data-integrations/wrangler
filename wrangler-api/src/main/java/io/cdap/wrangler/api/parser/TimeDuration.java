/*
 * Copyright © 2025 Cask Data, Inc.
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

import java.util.LinkedHashMap;
import java.util.Map;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Represents a duration of time, with methods to convert from string representations.
 */
public class TimeDuration implements Token {
    /**
     * The number of milliseconds represented by this TimeDuration instance.
     */
    private final long milliseconds;

    /**
     * A map of unit multipliers for converting time duration strings to milliseconds.
     */
    private static final Map<String, Long> MULTIPLIERS = new LinkedHashMap<>();

    static {
        MULTIPLIERS.put("H", 60L * 60 * 1000);
        MULTIPLIERS.put("M", 60L * 1000);
        MULTIPLIERS.put("S", 1000L);
        MULTIPLIERS.put("MS", 1L);
    }

    /**
     * Constructs a TimeDuration instance from a string representation.
     *
     * @param value the string representation of the time duration (e.g., "150ms")
     */
    public TimeDuration(String value) {
        this.milliseconds = parseMilliseconds(value.trim().toUpperCase());
    }

    /**
     * Parses a time duration string and converts it to milliseconds.
     *
     * @param value the time duration string to parse
     * @return the number of milliseconds
     * @throws IllegalArgumentException if the string is null, empty, or has an unknown unit
     */
    private long parseMilliseconds(String value) {
        for (Map.Entry<String, Long> entry : MULTIPLIERS.entrySet()) {
            String unit = entry.getKey();
            if (value.endsWith(unit)) {
                String numberPart = value.substring(0, value.length() - unit.length());
                try {
                    double number = Double.parseDouble(numberPart);
                    return (long) (number * entry.getValue());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid time duration number: " + numberPart);
                }
            }
        }
        throw new IllegalArgumentException("Unknown time duration unit in: " + value);
    }

    /**
     * Returns the number of milliseconds represented by this TimeDuration instance.
     *
     * @return the number of milliseconds
     */
    public long getMilliseconds() {
        return milliseconds;
    }

    @Override
    public Object value() {
        return this.milliseconds;
    }

    @Override
    public TokenType type() {
        return TokenType.NUMERIC;
    }

    @Override
    public JsonElement toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("type", type().name());
        json.addProperty("value", milliseconds);
        return json;
    }
}

