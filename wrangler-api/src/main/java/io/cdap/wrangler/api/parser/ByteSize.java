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

/**
 * The ByteSize class parses a token string representing a byte size (e.g., "10KB")
 * and provides a method to retrieve its value in bytes.
 */
public class ByteSize implements Token {
    private final double value;
    private final String unit;

    /**
     * Constructs a ByteSize token by parsing the given token string.
     *
     * @param token the token string, for example "10KB" or "1.5MB"
     */
    public ByteSize(String token) {
        int pos = 0;
        while (pos < token.length() &&
               (Character.isDigit(token.charAt(pos)) || token.charAt(pos) == '.')) {
            pos++;
        }
        this.value = Double.parseDouble(token.substring(0, pos));
        this.unit = token.substring(pos).toUpperCase();
    }

    /**
     * Converts the parsed value into bytes.
     *
     * @return the computed bytes as a long.
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
                return (long) (value * 1024 * 1024 * 1024 * 1024);
            default:
                throw new IllegalArgumentException("Unknown byte unit: " + unit);
        }
    }

    /**
     * Returns the value of this token. Here, we use the computed bytes.
     */
    @Override
    public Object value() {
        return getBytes();
    }

    /**
     * Returns the type of this token.
     *
     * @return TokenType.BYTE_SIZE
     */
    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    /**
     * Returns a JSON representation of this token.
     *
     * @return JsonElement representing this token.
     */
    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(getBytes());
    }
}