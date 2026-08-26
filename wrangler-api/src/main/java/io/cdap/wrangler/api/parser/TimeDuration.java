/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents a time duration token such as "100ms", "2s", "1.5min".
 * Converts supported time units into a canonical format: milliseconds.
 *
 * <p>Supported units: ms, s, sec, m, min</p>
 * <p>Throws {@link IllegalArgumentException} if the unit is invalid or the value is malformed.</p>
 *
 * <p>Implements {@link Token} for use in directive grammar parsing.</p>
 *
 * @see ByteSize
 * @see Token
 */
@PublicEvolving
public class TimeDuration implements Token {

    private static final Set<String> VALID_UNITS =
            new HashSet<>(Arrays.asList("ms", "s", "sec", "m", "min"));

    private final double value;
    private final String unit;

    /**
     * Constructs a {@code TimeDuration} from a string like "150ms", "2s", or "1.5min".
     *
     * @param token the string input representing a time duration
     * @throws IllegalArgumentException if the unit is invalid or number is malformed
     */
    public TimeDuration(String token) {
        token = token.trim().toLowerCase();
        this.unit = token.replaceAll("[0-9.]", "");

        if (!VALID_UNITS.contains(unit)) {
            throw new IllegalArgumentException("Invalid time duration unit: " + unit);
        }

        try {
            this.value = Double.parseDouble(token.replaceAll("[^0-9.]", ""));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value in TimeDuration: " + token);
        }
    }

    /**
     * Returns the canonical time value in milliseconds.
     *
     * @return the time value in milliseconds
     */
    public long getMilliseconds() {
        switch (unit) {
            case "ms":
                return (long) value;
            case "s":
            case "sec":
                return (long) (value * 1000);
            case "m":
            case "min":
                return (long) (value * 60 * 1000);
            default:
                throw new IllegalStateException("Unhandled time unit: " + unit);
        }
    }

    @Override
    public Object value() {
        return getMilliseconds();
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", TokenType.TIME_DURATION.name());
        object.addProperty("value", getMilliseconds());
        return object;
    }
}
