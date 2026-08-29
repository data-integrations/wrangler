/*
 * Copyright © 2025 Cask Data, Inc.
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

import java.util.LinkedHashMap;
import java.util.Map;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Represents a size in bytes, with methods to convert from string representations.
 */
public class ByteSize implements Token {
    /**
     * The number of bytes represented by this ByteSize instance.
     */
    private final long bytes;

    /**
     * A map of unit multipliers for converting byte size strings to bytes.
     */
    private static final Map<String, Long> MULTIPLIERS = new LinkedHashMap<>();

    static {
        MULTIPLIERS.put("TB", 1024L * 1024 * 1024 * 1024);
        MULTIPLIERS.put("GB", 1024L * 1024 * 1024);
        MULTIPLIERS.put("MB", 1024L * 1024);
        MULTIPLIERS.put("KB", 1024L);
        MULTIPLIERS.put("B", 1L);
    }

    /**
     * Constructs a ByteSize instance from a string representation.
     *
     * @param value the string representation of the byte size (e.g., "10KB")
     */
    public ByteSize(String value) {
        this.bytes = parseBytes(value.trim().toUpperCase());
    }

    /**
     * Parses a byte size string and converts it to bytes.
     *
     * @param value the byte size string to parse
     * @return the number of bytes
     * @throws IllegalArgumentException if the string is null, empty, or has an unknown unit
     */
    private long parseBytes(String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Byte size value cannot be null or empty.");
        }

        for (Map.Entry<String, Long> entry : MULTIPLIERS.entrySet()) {
            String unit = entry.getKey();
            if (value.endsWith(unit)) {
                String numberPart = value.substring(0, value.length() - unit.length());
                try {
                    double number = Double.parseDouble(numberPart);
                    return (long) (number * entry.getValue());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid byte size number: " + numberPart);
                }
            }
        }
        throw new IllegalArgumentException("Unknown byte unit in: " + value);
    }

    /**
     * Returns the number of bytes represented by this ByteSize instance.
     *
     * @return the number of bytes
     */
    public long getBytes() {
        return bytes;
    }

    @Override
    public Object value() {
        return this.bytes;
    }

    @Override
    public TokenType type() {
        return TokenType.NUMERIC;
    }

    @Override
    public JsonElement toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("type", type().name());
        json.addProperty("value", bytes);
        return json;
    }
}
