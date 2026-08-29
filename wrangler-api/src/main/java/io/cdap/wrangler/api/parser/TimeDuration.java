/*
 * Copyright © 2017-2025 Cask Data, Inc.
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

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A token representing a time duration value (e.g., "150ms", "2.1s").
 */
@PublicEvolving
public class TimeDuration implements Token {
    private final String value;
    private final long nanos;
    private static final Pattern PATTERN = Pattern.compile("^(\\d+\\.?\\d*)\\s*([a-zA-Z]+)$");
    private static final long NS = 1L;
    private static final long US = NS * 1000L;
    private static final long MS = US * 1000L;
    private static final long S = MS * 1000L;
    private static final long MIN = S * 60L;
    private static final long H = MIN * 60L;
    private static final long D = H * 24L;

    public TimeDuration(String value) {
        this.value = value;
        this.nanos = parseNanos(value);
    }

    /**
     * Parses the input string to compute the duration in nanoseconds.
     *
     * @param input the time duration string (e.g., "150ms")
     * @return the duration in nanoseconds
     * @throws IllegalArgumentException if the input is invalid
     */
    private long parseNanos(String input) {
        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException("Time duration cannot be null or empty");
        }
        Matcher matcher = PATTERN.matcher(input.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid time duration format: " + input);
        }

        BigDecimal number;
        try {
            number = new BigDecimal(matcher.group(1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number format in time duration: " + matcher.group(1), e);
        }
        String unit = matcher.group(2).toLowerCase();

        switch (unit) {
            case "ns":
                return number.longValueExact();
            case "us":
                return number.multiply(new BigDecimal(US)).longValueExact();
            case "ms":
                return number.multiply(new BigDecimal(MS)).longValueExact();
            case "s":
                return number.multiply(new BigDecimal(S)).longValueExact();
            case "min":
                return number.multiply(new BigDecimal(MIN)).longValueExact();
            case "h":
                return number.multiply(new BigDecimal(H)).longValueExact();
            case "d":
                return number.multiply(new BigDecimal(D)).longValueExact();
            default:
                throw new IllegalArgumentException("Unknown time unit: " + unit);
        }
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
        object.addProperty("nanos", nanos);
        return object;
    }

    /**
     * Returns the duration in nanoseconds.
     *
     * @return the duration in nanoseconds
     */
    public long getNanos() {
        return nanos;
    }
}