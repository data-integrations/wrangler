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
/**
 * Token for representing byte sizes like "10KB", "1MB", etc.
 */

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimeDuration implements Token {
    private static final Pattern PATTERN = Pattern.compile("(?i)(\\d+(\\.\\d+)?)(ns|us|ms|s|m|h|d)");

    private final String original;
    private final long millis;

    public TimeDuration(String value) {
        this.original = value;

        Matcher matcher = PATTERN.matcher(value.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid time duration: " + value);
        }

        double number = Double.parseDouble(matcher.group(1));
        String unit = matcher.group(3).toLowerCase();

        switch (unit) {
            case "ns": millis = (long) (number / 1_000_000); break;
            case "us": millis = (long) (number / 1_000); break;
            case "ms": millis = (long) number; break;
            case "s":  millis = (long) (number * 1000); break;
            case "m":  millis = (long) (number * 60 * 1000); break;
            case "h":  millis = (long) (number * 60 * 60 * 1000); break;
            case "d":  millis = (long) (number * 24 * 60 * 60 * 1000); break;
            default: throw new IllegalArgumentException("Unknown time unit: " + unit);
        }
    }

    public long getMillis() {
        return millis;
    }

    @Override
    public Object value() {
        return original;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(original);
    }
}
