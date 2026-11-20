/*
 * Copyright © 2023 Cask Data, Inc.
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

/**
 * The ByteSize class represents a size in bytes with support for various units
 * (B, KB, MB, GB, TB).
 * It implements the Token interface for use in the wrangler parser system.
 */
@PublicEvolving
public class ByteSize implements Token {
    private static final Pattern BYTE_SIZE_PATTERN = Pattern.compile("^(\\d+(?:\\.\\d+)?)(B|KB|MB|GB|TB)$",
            Pattern.CASE_INSENSITIVE);
    private final String value;
    private final long bytes;

    /**
     * Creates a new ByteSize instance from a string representation.
     *
     * @param value The string representation of the byte size (e.g., "1KB",
     *              "1.5MB", "2GB")
     * @throws IllegalArgumentException if the value is not in a valid format
     */
    public ByteSize(String value) {
        this.value = value;
        this.bytes = parseBytes(value);
    }

    /**
     * Parses the string representation into bytes.
     *
     * @param value The string to parse
     * @return The number of bytes
     * @throws IllegalArgumentException if the value is not in a valid format
     */
    private long parseBytes(String value) {
        Matcher matcher = BYTE_SIZE_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid byte size format: " + value);
        }

        double number = Double.parseDouble(matcher.group(1));
        String unit = matcher.group(2).toUpperCase();

        switch (unit) {
            case "B":
                return (long) number;
            case "KB":
                return (long) (number * 1024);
            case "MB":
                return (long) (number * 1024 * 1024);
            case "GB":
                return (long) (number * 1024 * 1024 * 1024);
            case "TB":
                return (long) (number * 1024 * 1024 * 1024 * 1024);
            default:
                throw new IllegalArgumentException("Unsupported unit: " + unit);
        }
    }

    /**
     * Gets the number of bytes represented by this ByteSize.
     *
     * @return The number of bytes
     */
    public long getBytes() {
        return bytes;
    }

    /**
     * Gets the size in kilobytes.
     *
     * @return The size in kilobytes
     */
    public double getKilobytes() {
        return bytes / 1024.0;
    }

    /**
     * Gets the size in megabytes.
     *
     * @return The size in megabytes
     */
    public double getMegabytes() {
        return bytes / (1024.0 * 1024.0);
    }

    /**
     * Gets the size in gigabytes.
     *
     * @return The size in gigabytes
     */
    public double getGigabytes() {
        return bytes / (1024.0 * 1024.0 * 1024.0);
    }

    /**
     * Gets the size in terabytes.
     *
     * @return The size in terabytes
     */
    public double getTerabytes() {
        return bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0);
    }

    @Override
    public String value() {
        return value;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", TokenType.BYTE_SIZE.name());
        object.addProperty("value", value);
        return object;
    }
}
