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
 * Represents a duration of time.
 * This class provides methods to parse and format time durations.
 */
public class TimeDuration implements Token {
    private static final Pattern PATTERN = Pattern.compile("(\\d+(\\.\\d+)?)(ms|s|m|h|d)", Pattern.CASE_INSENSITIVE);
    private final long milliseconds;

    public TimeDuration(String value) throws IllegalArgumentException {
        Matcher matcher = PATTERN.matcher(value.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid time duration format: " + value);
        }

        double number = Double.parseDouble(matcher.group(1));
        String unit = matcher.group(3).toLowerCase();

        switch (unit) {
            case "ms":
                this.milliseconds = (long) number;
                break;
            case "s":
                this.milliseconds = (long) (number * 1000);
                break;
            case "m":
                this.milliseconds = (long) (number * 1000 * 60);
                break;
            case "h":
                this.milliseconds = (long) (number * 1000 * 60 * 60);
                break;
            case "d":
                this.milliseconds = (long) (number * 1000 * 60 * 60 * 24);
                break;
            default:
                throw new IllegalArgumentException("Unsupported time duration unit: " + unit);
        }
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

    public long getMilliseconds() {
        return milliseconds;
    }
}
