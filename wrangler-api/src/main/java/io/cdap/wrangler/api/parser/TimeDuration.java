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
import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * The TimeDuration class represents a time duration value with its unit (ms, s,
 * m, h).
 * It implements the Token interface and provides methods to convert between
 * different time units.
 */
@PublicEvolving
public class TimeDuration implements Token {
    private final long milliseconds;
    private final String originalValue;

    public TimeDuration(String value) {
        this.originalValue = value;
        this.milliseconds = parseTimeDuration(value);
    }

    public TimeDuration(long milliseconds, String unit) {
        this.milliseconds = milliseconds;
        this.originalValue = formatTimeDuration(milliseconds, unit);
    }

    @Override
    public Long value() {
        return milliseconds;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", TokenType.TIME_DURATION.name());
        object.addProperty("value", originalValue);
        object.addProperty("milliseconds", milliseconds);
        return object;
    }

    /**
     * Gets the value in milliseconds
     * 
     * @return the value in milliseconds
     */
    public long getMilliseconds() {
        return milliseconds;
    }

    /**
     * Gets the original string representation
     * 
     * @return the original string value
     */
    public String getOriginalValue() {
        return originalValue;
    }

    /**
     * Formats a time duration value with the given unit
     * 
     * @param milliseconds the value in milliseconds
     * @param unit         the target unit (ms, s, m, h)
     * @return the formatted string
     */
    private String formatTimeDuration(long milliseconds, String unit) {
        long multiplier = getMultiplier(unit);
        double value = (double) milliseconds / multiplier;
        return String.format("%.0f%s", value, unit);
    }

    /**
     * Parses a time duration string into milliseconds
     * 
     * @param value the time duration string (e.g., "100ms", "1.5s", "2m")
     * @return the value in milliseconds
     * @throws IllegalArgumentException if the format is invalid
     */
    private long parseTimeDuration(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Time duration value cannot be null or empty");
        }

        value = value.trim().toLowerCase();
        int unitIndex = -1;
        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i)) && value.charAt(i) != '.') {
                unitIndex = i;
                break;
            }
        }

        if (unitIndex == -1) {
            return Long.parseLong(value);
        }

        String numberStr = value.substring(0, unitIndex);
        String unit = value.substring(unitIndex);

        double number = Double.parseDouble(numberStr);
        long multiplier = getMultiplier(unit);

        return Math.round(number * multiplier);
    }

    /**
     * Gets the multiplier for a time unit
     * 
     * @param unit the unit (ms, s, m, h)
     * @return the multiplier in milliseconds
     * @throws IllegalArgumentException if the unit is not supported
     */
    private long getMultiplier(String unit) {
        switch (unit) {
            case "ms":
                return 1L;
            case "s":
                return 1000L;
            case "m":
                return 60L * 1000L;
            case "h":
                return 60L * 60L * 1000L;
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + unit);
        }
    }
}