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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@PublicEvolving
public class TimeDuration implements Token {
    private long millis;

    // Regular expression to match time duration formats like "10s", "5m", "3h", "1d"
    private static final Pattern TIME_DURATION_PATTERN = Pattern.compile("^(\\d+)(ms|s|m|h|d)?$", Pattern.CASE_INSENSITIVE);

    /**
     * Constructor to initialize the TimeDuration object by parsing the input token string.
     *
     * @param token The token representing a time duration (e.g., "10s", "5m", "3h").
     * @throws IllegalArgumentException if the token does not match the expected format.
     */
    public TimeDuration(String token) {
        Matcher matcher = TIME_DURATION_PATTERN.matcher(token);
        if (matcher.matches()) {
            long value = Long.parseLong(matcher.group(1));
            String unit = matcher.group(2);
            switch (unit == null ? "" : unit.toLowerCase()) {
                case "ms":
                    this.millis = value;
                    break;
                case "s":
                    this.millis = value * 1000;
                    break;
                case "m":
                    this.millis = value * 1000 * 60;
                    break;
                case "h":
                    this.millis = value * 1000 * 60 * 60;
                    break;
                case "d":
                    this.millis = value * 1000 * 60 * 60 * 24;
                    break;
                default:
                    throw new IllegalArgumentException("Invalid TimeDuration format");
            }
        } else {
            throw new IllegalArgumentException("Invalid TimeDuration format: " + token);
        }
    }

    /**
     * Returns the value of the time duration in milliseconds.
     *
     * @return The time duration in milliseconds as a Long.
     */
    @Override
    public Long value() {
        return Long.valueOf(millis);  // Return as Long object to match the Token interface
    }

    /**
     * Returns the type of the token (TimeDuration).
     *
     * @return The TokenType for TimeDuration.
     */
    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    /**
     * Converts the TimeDuration object to its JSON representation.
     *
     * @return The JSON representation of the TimeDuration object.
     */
    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", TokenType.TIME_DURATION.name());
        object.addProperty("value", millis);
        return object;
    }

    /**
     * Gets the time duration in milliseconds.
     *
     * @return The time duration in milliseconds.
     */
    public long getMillis() {
        return millis;
    }
}
