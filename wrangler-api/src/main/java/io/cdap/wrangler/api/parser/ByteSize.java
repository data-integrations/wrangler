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

import java.util.Locale;

/**
 * Represents a token for byte size values with units (e.g., KB, MB, GB).
 */
public class ByteSize implements Token {

    private Double bytes;

    /**
     * Constructs a ByteSize token.
     *
     * @param value The string representation of byte size (e.g., "10KB").
     */
    public ByteSize(String value) {
        this.bytes = parseBytes(value);
    }

    /**
     * Returns the value of this {@code ByteSize} object as a {@code Double}
     * representing the size in bytes.
     *
     * @return the byte size value of this object.
     */
    @Override
    public Double value() {
        return getBytes();
    }

    /**
     * Returns the value in bytes.
     *
     * @return Byte size in long.
     */
    public double getBytes() {
        return this.bytes;
    }

    /**
     * Returns the type of this {@code BYTE_SIZE} object as a {@code TokenType}
     * enum.
     *
     * @return the enumerated {@code TokenType} of this object.
     */
    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    /**
     * Parses the input string and converts it to bytes.
     *
     * @param value The input byte size string.
     * @return The size in bytes.
     * @throws IllegalArgumentException if input is not a valid byte size
     * string.
     */
    private Double parseBytes(String value) {
        String normalize = value.trim().toLowerCase(Locale.ENGLISH);
        double valueDouble = Double.parseDouble(normalize.replaceAll("[^0-9.-]", ""));
        String unit = normalize.replaceAll("[^a-zA-Z]+", "");
        System.out.println(unit);

        if (valueDouble < 0) {
            throw new IllegalArgumentException("Negative byte size values are not allowed: " + value);
        }

        if (unit.equals("bytes")) {
            return valueDouble;
        } else if (unit.equals("b")) {
            return valueDouble;
        } else if (unit.equals("kb")) {
            return valueDouble * 1024;
        } else if (unit.equals("mb")) {
            return valueDouble * 1024 * 1024;
        } else if (unit.equals("gb")) {
            return valueDouble * 1024 * 1024 * 1024;
        } else if (unit.equals("tb")) {
            return valueDouble * 1024 * 1024 * 1024 * 1024;
        } else {
            throw new IllegalArgumentException("Unsupported byte unit in: " + value);
        }
    }

    /**
     * Returns the members of this {@code BYTE_SIZE} object as a
     * {@code JsonElement}.
     *
     * @return Json representation of this {@code BYTE_SIZE} object as
     * {@code JsonElement}
     */
    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", TokenType.BYTE_SIZE.name());
        object.addProperty("value", this.bytes);
        return object;
    }
}
