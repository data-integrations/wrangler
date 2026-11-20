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

/**
 * Token class representing byte size values with units (e.g., "10KB", "5MB").
 * Parses and stores byte sizes, providing methods to retrieve the value in
 * bytes.
 */
@PublicEvolving
public class ByteSize implements Token {
    private static final Pattern BYTE_SIZE_PATTERN = Pattern.compile("(\\d+)\\s*([kKmMgGtTpP]?[bB]?)");
    private final String value;
    private final long bytes;

    /**
     * Constructs a ByteSize token from a string representation.
     * Accepts formats like "10KB", "5MB", "1GB", etc.
     *
     * @param value String representation of a byte size with unit
     * @throws IllegalArgumentException if the string cannot be parsed as a byte
     *                                  size
     */
    public ByteSize(String value) {
        this.value = value;
        this.bytes = parseBytes(value);
    }

    /**
     * Parses a string representation of byte size into its byte value.
     *
     * @param sizeStr String representation of a byte size (e.g., "10KB")
     * @return The size in bytes
     * @throws IllegalArgumentException if the string cannot be parsed
     */
    private long parseBytes(String sizeStr) {
        Matcher matcher = BYTE_SIZE_PATTERN.matcher(sizeStr);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid byte size format: " + sizeStr);
        }

        long size = Long.parseLong(matcher.group(1));
        String unit = matcher.group(2).toUpperCase();

        switch (unit) {
            case "B":
            case "":
                return size;
            case "KB":
            case "K":
                return size * 1024;
            case "MB":
            case "M":
                return size * 1024 * 1024;
            case "GB":
            case "G":
                return size * 1024 * 1024 * 1024;
            case "TB":
            case "T":
                return size * 1024 * 1024 * 1024 * 1024;
            case "PB":
            case "P":
                return size * 1024 * 1024 * 1024 * 1024 * 1024;
            default:
                throw new IllegalArgumentException("Unsupported byte size unit: " + unit);
        }
    }

    /**
     * Returns the original string representation of the byte size.
     */
    @Override
    public String value() {
        return value;
    }

    /**
     * Returns the size in bytes.
     *
     * @return The size in bytes
     */
    public long getBytes() {
        return bytes;
    }

    /**
     * Returns the size in kilobytes.
     *
     * @return The size in kilobytes
     */
    public double getKilobytes() {
        return bytes / 1024.0;
    }

    /**
     * Returns the size in megabytes.
     *
     * @return The size in megabytes
     */
    public double getMegabytes() {
        return bytes / (1024.0 * 1024.0);
    }

    /**
     * Returns the size in gigabytes.
     *
     * @return The size in gigabytes
     */
    public double getGigabytes() {
        return bytes / (1024.0 * 1024.0 * 1024.0);
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
        object.addProperty("bytes", bytes);
        return object;
    }
}