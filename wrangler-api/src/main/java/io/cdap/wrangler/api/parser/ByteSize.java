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
 * The ByteSize class wraps byte size values such as {@code "10KB"} or {@code "1.5MB"}
 * and converts them into a canonical format, i.e., bytes.
 *
 * <p>It implements the {@code Token} interface, providing methods to retrieve
 * the token type and the parsed byte value, and to serialize the token as JSON.</p>
 *
 * @see TimeDuration
 * @see Numeric
 * @see Text
 */

@PublicEvolving
public class ByteSize implements Token {
    private final double value;
    private final String unit;

    /**
     * Constructs a new {@code ByteSize} token from a string.
     *
     * @param token the string representation (e.g. "10MB", "1.2GB").
     */
    public ByteSize(String token) {
        token = token.trim().toUpperCase();
        this.unit = token.replaceAll("[0-9.]", "");
        this.value = Double.parseDouble(token.replaceAll("[^0-9.]", ""));
    }

    /**
     * Returns the byte size in canonical form (bytes).
     *
     * @return the value in bytes as a {@code long}.
     */
    public long getBytes() {
        switch (unit) {
            case "B":
                return (long) value;
            case "KB":
                return (long) (value * 1024);
            case "MB":
                return (long) (value * 1024 * 1024);
            case "GB":
                return (long) (value * 1024 * 1024 * 1024);
            case "TB":
                return (long) (value * 1024L * 1024 * 1024 * 1024);
            default:
                return (long) value;
        }
    }

    @Override
    public Object value() {
        return getBytes();
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("type", TokenType.BYTE_SIZE.name());
        jsonObject.addProperty("value", value);
        return jsonObject;
    }
}
