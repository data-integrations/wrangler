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
 * Represents a byte size value with support for different units (B, KB, MB, GB).
 * This class parses string representations of byte sizes and provides methods to access the value in bytes.
 */
public class ByteSize implements Token {
    private final String original;
    private final long bytes;

    /**
     * Creates a new ByteSize instance from a string token.
     *
     * @param token the string token representing a byte size (e.g., "1KB", "2MB", "3GB")
     */
    public ByteSize(String token) {
        this.original = token;
        this.bytes = parseByteSize(token);
    }

    /**
     * Parses a string representation of a byte size into a long value.
     *
     * @param input the string to parse
     * @return the byte size in bytes
     * @throws IllegalArgumentException if the input format is invalid
     */
    private long parseByteSize(String input) {
        input = input.trim().toUpperCase();
        if (input.endsWith("KB")) {
            return Long.parseLong(input.replace("KB", "")) * 1024;
        }
        if (input.endsWith("MB")) {
            return Long.parseLong(input.replace("MB", "")) * 1024 * 1024;
        }
        if (input.endsWith("GB")) {
            return Long.parseLong(input.replace("GB", "")) * 1024 * 1024 * 1024;
        }
        if (input.endsWith("B")) {
            return Long.parseLong(input.replace("B", ""));
        }
        throw new IllegalArgumentException("Invalid size format: " + input);
    }

    @Override
    public Object value() {
        return bytes;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    /**
     * Gets the byte size value.
     *
     * @return the byte size in bytes
     */
    public long getBytes() {
        return bytes;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(bytes);
    }

    /**
     * Format the byte size to a specified unit.
     *
     * @param unit The target unit (B, KB, MB, GB)
     * @return The formatted string with the specified unit
     * @throws IllegalArgumentException if the unit is not one of: B, KB, MB, GB
     */
    public String format(String unit) {
        double value = bytes;
        switch (unit.toUpperCase()) {
            case "B":
                return String.format("%.0fB", value);
            case "KB":
                return String.format("%.2fKB", value / 1024);
            case "MB":
                return String.format("%.2fMB", value / (1024 * 1024));
            case "GB":
                return String.format("%.2fGB", value / (1024 * 1024 * 1024));
            default:
                throw new IllegalArgumentException("Invalid unit: " + unit + ". Must be one of: B, KB, MB, GB");
        }
    }
}
