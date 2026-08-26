/*
 * Copyright © 2025 Nitin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimeDuration implements Token {
    private static final Pattern PATTERN = Pattern.compile("(?i)(\\d+(\\.\\d+)?)(ms|s|m(in)?|h|d)");
    private final double value;
    private final String unit;

    public TimeDuration(String input) {
        Matcher matcher = PATTERN.matcher(input.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid time duration: " + input);
        }
        this.value = Double.parseDouble(matcher.group(1));
        this.unit = matcher.group(3).toLowerCase();
    }

    public long toMilliseconds() {
        switch (unit) {
            case "ms":
                return (long) value;
            case "s":
                return (long) (value * 1000);
            case "m":
            case "min":
                return (long) (value * 60 * 1000);
            case "h":
                return (long) (value * 60 * 60 * 1000);
            case "d":
                return (long) (value * 24 * 60 * 60 * 1000);
            default:
                throw new IllegalStateException("Unexpected unit: " + unit);
        }
    }

    @Override
    public Object value() {
        return value + unit;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        JsonObject obj = new JsonObject();
        obj.addProperty("type", type().name());
        obj.addProperty("value", String.valueOf(value()));
        obj.addProperty("milliseconds", toMilliseconds());
        return obj;
    }

    @Override
    public String toString() {
        return value() + "";
    }
}

