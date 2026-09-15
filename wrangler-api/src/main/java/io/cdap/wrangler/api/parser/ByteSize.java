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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a ByteSize token used in parsing directives.
 * Accepts strings like "100B", "10KB", "5MB", "2GB", "1TB", "3PB"
 * and converts them to bytes.
 */
public class ByteSize implements Token {

    private static final Pattern SIZE_PATTERN = 
      Pattern.compile("^(\\d+)\\s*(PB|TB|GB|MB|KB|B)$", Pattern.CASE_INSENSITIVE);
    private final long bytes;

    /**
     * Create a ByteSize from a string representation.
     *
     * @param input The string representation of byte size (e.g., "10MB")
     * @throws IllegalArgumentException if the input format is invalid
     */
    public ByteSize(String input) {
        this.bytes = parseBytes(input);
    }

    /**
     * Parses a human-readable byte size string into its equivalent bytes.
     *
     * Supported formats:
     * - "B" for bytes
     * - "KB" for kilobytes (1024 bytes)
     * - "MB" for megabytes (1024^2 bytes)
     * - "GB" for gigabytes (1024^3 bytes)
     * - "TB" for terabytes (1024^4 bytes)
     * - "PB" for petabytes (1024^5 bytes)
     *
     * @param input The input string like "10MB", "2GB", "5TB", etc.
     * @return The number of bytes as a long
     * @throws IllegalArgumentException if the unit is unrecognized or format is invalid
     */
    private long parseBytes(String input) {
        if (input == null) {
            throw new IllegalArgumentException("Input cannot be null");
        }
        
        input = input.trim().toUpperCase();
        Matcher matcher = SIZE_PATTERN.matcher(input);
        
        if (matcher.matches()) {
            long value = Long.parseLong(matcher.group(1));
            String unit = matcher.group(2);
            
            switch (unit) {
                case "PB":
                    return value * 1024L * 1024L * 1024L * 1024L * 1024L;
                case "TB":
                    return value * 1024L * 1024L * 1024L * 1024L;
                case "GB":
                    return value * 1024L * 1024L * 1024L;
                case "MB":
                    return value * 1024L * 1024L;
                case "KB":
                    return value * 1024L;
                case "B":
                    return value;
                default:
                    // Should never happen due to regex pattern
                    throw new IllegalArgumentException("Unsupported size unit: " + unit);
            }
        }
        
        throw new IllegalArgumentException("Invalid byte size format: " + input);
    }

    /**
     * Converts a byte count to the specified unit.
     *
     * @param bytes The number of bytes
     * @param unit The target unit (b, kb, mb, gb, tb, pb)
     * @return The byte count converted to the specified unit
     * @throws IllegalArgumentException if the unit is not supported
     */
    public static long convertBytesToUnit(long bytes, String unit) {
        if (unit == null) {
            throw new IllegalArgumentException("Unit cannot be null");
        }
        
        switch (unit.toLowerCase()) {
            case "b":
                return bytes;
            case "kb":
                return bytes / 1024L;
            case "mb":
                return bytes / (1024L * 1024L);
            case "gb":
                return bytes / (1024L * 1024L * 1024L);
            case "tb":
                return bytes / (1024L * 1024L * 1024L * 1024L);
            case "pb":
                return bytes / (1024L * 1024L * 1024L * 1024L * 1024L);
            default:
                throw new IllegalArgumentException("Unsupported size unit: " + unit);
        }
    }

    /**
     * @return The size in bytes
     */
    public long getBytes() {
        return bytes;
    }

    @Override
    public Object value() {
        return bytes;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(bytes);
    }
}
