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
 * The TimeDuration class wraps time duration values with unit conversion capabilities.
 * An object of type {@code TimeDuration} contains the value in nanoseconds as well as
 * the original string representation.
 */
@PublicEvolving
public class TimeDuration implements Token {
    private final long nanoseconds;
    private final String originalValue;

    public TimeDuration(String value) {
        this.originalValue = value;
        this.nanoseconds = parseNanoseconds(value);
    }

    private long parseNanoseconds(String value) {
        String trimmed = value.trim();
        int lastDigitIndex = -1;
        for (int i = 0; i < trimmed.length(); i++) {
            if (!Character.isDigit(trimmed.charAt(i)) && trimmed.charAt(i) != '.') {
                lastDigitIndex = i;
                break;
            }
        }
        if (lastDigitIndex == -1) {
            throw new IllegalArgumentException("Invalid time duration format: " + value);
        }

        double number = Double.parseDouble(trimmed.substring(0, lastDigitIndex));
        String unit = trimmed.substring(lastDigitIndex).trim().toLowerCase();

        switch (unit) {
            case "ns":
                return (long) number;
            case "μs":
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
                throw new IllegalArgumentException("Unknown time duration unit: " + unit);
        }
    }

    public long getNanoseconds() {
        return nanoseconds;
    }

    public double toSeconds() {
        return nanoseconds / (1000.0 * 1000.0 * 1000.0);
    }

    @Override
    public String value() {
        return originalValue;
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
        object.addProperty("nanoseconds", nanoseconds);
        return object;
    }

    public double getMicroseconds() {
        return nanoseconds / 1000.0;
    }

    public double getMilliseconds() {
        return nanoseconds / (1000.0 * 1000);
    }

    public double getSeconds() {
        return nanoseconds / (1000.0 * 1000 * 1000);
    }

    public double getMinutes() {
        return nanoseconds / (60.0 * 1000 * 1000 * 1000);
    }

    public double getHours() {
        return nanoseconds / (60.0 * 60 * 1000 * 1000 * 1000);
    }

    public double getDays() {
        return nanoseconds / (24.0 * 60 * 60 * 1000 * 1000 * 1000);
    }
} 

