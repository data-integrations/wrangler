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

public class TimeDuration implements Token {
    private static final Pattern TIME_PATTERN = Pattern.compile("^(\\d+\\.?\\d*)\\s*(ns|us|ms|s|m|h)$", Pattern.CASE_INSENSITIVE);
    private final String original;
    private final long nanoseconds;

    public TimeDuration(String value) {
        this.original = value;
        this.nanoseconds = parseDuration(value);
    }

    private long parseDuration(String input) {
        Matcher matcher = TIME_PATTERN.matcher(input.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid time duration format: " + input);
        }

        double value = Double.parseDouble(matcher.group(1));
        String unit = matcher.group(2).toLowerCase();

        switch (unit) {
            case "ns": return (long) value;
            case "us": return (long) (value * 1_000);
            case "ms": return (long) (value * 1_000_000);
            case "s": return (long) (value * 1_000_000_000);
            case "m": return (long) (value * 60 * 1_000_000_000L);
            case "h": return (long) (value * 60 * 60 * 1_000_000_000L);
            default: throw new IllegalArgumentException("Unsupported time unit: " + unit);
        }
    }

    // Required by Token interface
    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public Object value() {
        return nanoseconds;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(nanoseconds);
    }

    // Additional convenience methods
    public long getNanoSeconds() {
        return nanoseconds;
    }

    public long getMilliSeconds() {
        return nanoseconds / 1_000_000;
    }

    public String getOriginal() {
        return original;
    }
}