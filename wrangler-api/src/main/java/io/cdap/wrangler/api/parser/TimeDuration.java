/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
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
import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * The TimeDuration class wraps duration values such as {@code "150ms"} or {@code "2min"}
 * and converts them into a canonical format, i.e., milliseconds.
 *
 * <p>This class implements the {@code Token} interface, exposing methods to extract the
 * canonical value and token type, and to serialize the token into JSON.</p>
 *
 * @see ByteSize
 * @see Numeric
 * @see Text
 */
@PublicEvolving
public class TimeDuration implements Token {
    private final double value;
    private final String unit;

    /**
     * Constructs a new {@code TimeDuration} token from a string.
     *
     * @param token the string representation (e.g. "150ms", "2s").
     */
    public TimeDuration(String token) {
        token = token.trim().toLowerCase();
        this.unit = token.replaceAll("[0-9.]", "");
        this.value = Double.parseDouble(token.replaceAll("[^0-9.]", ""));
    }

    /**
     * Returns the time duration in canonical form (milliseconds).
     *
     * @return the value in milliseconds as a {@code long}.
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
                return (long) value;
        }
    }

    @Override
    public Object value() {
        return getMilliseconds(); // return canonical value
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
