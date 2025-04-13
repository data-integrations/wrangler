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

/**
 * Represents a time duration token parsed from a directive expression.
 */
public class TimeDuration implements Token {
    private final String value;
    private final long milliseconds;

    public TimeDuration(String value) {
        this.value = value;
        this.milliseconds = parseMilliseconds(value);
    }

    private long parseMilliseconds(String value) {
        value = value.trim().toLowerCase();
        long multiplier = 1;
        if (value.endsWith("ms")) {
            multiplier = 1L;
            value = value.replace("ms", "");
        } else if (value.endsWith("s")) {
            multiplier = 1000L;
            value = value.replace("s", "");
        } else if (value.endsWith("min")) {
            multiplier = 60_000L;
            value = value.replace("min", "");
        } else if (value.endsWith("h")) {
            multiplier = 3_600_000L;
            value = value.replace("h", "");
        }

        try {
            return Long.parseLong(value.trim()) * multiplier;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid time duration format: " + this.value, e);
        }
    }

    public long getMilliseconds() {
        return milliseconds;
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
        object.addProperty("milliseconds", milliseconds);
        return object;
    }
}
