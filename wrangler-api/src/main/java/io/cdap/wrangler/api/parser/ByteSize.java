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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ByteSize is a Token implementation that represents a byte size value with a unit.
 * It can parse strings like "10KB", "1.5MB", "2GB", etc.
 */
public class ByteSize implements Token {
    // Regex pattern to match a number followed by a byte unit
    private static final Pattern PATTERN = Pattern.compile("^(\\d+(?:\\.\\d+)?)\\s*(B|KB|MB|GB|TB)$", Pattern.CASE_INSENSITIVE);

    // The numeric value
    private final double value;

    // The unit (B, KB, MB, GB, TB)
    private final String unit;

    // The number of bytes
    private final long bytes;

    /**
     * Constructs a ByteSize token by parsing the input string.
     *
     * @param value The string representation of the byte size (e.g. "10KB", "1.5MB")
     */
    public ByteSize(String value) {
        super();

        Matcher matcher = PATTERN.matcher(value.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    String.format("Invalid byte size format: '%s'. Expected format: number followed by B, KB, MB, GB, or TB", value));
        }

        this.value = Double.parseDouble(matcher.group(1));
        this.unit = matcher.group(2).toUpperCase();
        this.bytes = calculateBytes(this.value, this.unit);
    }

    /**
     * Calculates the number of bytes based on the value and unit.
     * Uses binary conversion: 1 KB = 1024 bytes, 1 MB = 1024^2 bytes, etc.
     *
     * @param value The numeric value
     * @param unit The unit (B, KB, MB, GB, TB)
     * @return The number of bytes
     */
    private long calculateBytes(double value, String unit) {
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
     * Gets the raw numeric value.
     *
     * @return The numeric value without the unit
     */
    public double getValue() {
        return value;
    }

    /**
     * Gets the unit (B, KB, MB, GB, TB).
     *
     * @return The unit as a string
     */
    public String getUnit() {
        return unit;
    }

    /**
     * Gets the size in bytes.
     *
     * @return The number of bytes
     */
    public long getBytes() {
        return bytes;
    }

    /**
     * Gets the size in kilobytes.
     *
     * @return The number of kilobytes
     */
    public double getKilobytes() {
        return bytes / 1024.0;
    }

    /**
     * Gets the size in megabytes.
     *
     * @return The number of megabytes
     */
    public double getMegabytes() {
        return bytes / (1024.0 * 1024.0);
    }

    /**
     * Gets the size in gigabytes.
     *
     * @return The number of gigabytes
     */
    public double getGigabytes() {
        return bytes / (1024.0 * 1024.0 * 1024.0);
    }

    /**
     * Gets the size in terabytes.
     *
     * @return The number of terabytes
     */
    public double getTerabytes() {
        return bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0);
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
        return new com.google.gson.JsonPrimitive(getBytes());
    }
}