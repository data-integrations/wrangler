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

/**
 * The ByteSize class represents a byte size value with its unit (KB, MB, GB,
 * etc.).
 * It implements the Token interface and provides methods to convert between
 * different byte size units.
 */
@PublicEvolving
public class ByteSize implements Token {
    private final long bytes;
    private final String originalValue;

    public ByteSize(String value) {
        this.originalValue = value;
        this.bytes = parseByteSize(value);
    }

    public ByteSize(long bytes, String unit) {
        this.bytes = bytes;
        this.originalValue = formatByteSize(bytes, unit);
    }

    @Override
    public Long value() {
        return bytes;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", TokenType.BYTE_SIZE.name());
        object.addProperty("value", originalValue);
        object.addProperty("bytes", bytes);
        return object;
    }

    /**
     * Gets the value in bytes
     * 
     * @return the value in bytes
     */
    public long getBytes() {
        return bytes;
    }

    /**
     * Gets the original string representation
     * 
     * @return the original string value
     */
    public String getOriginalValue() {
        return originalValue;
    }

    /**
     * Formats a byte size value with the given unit
     * 
     * @param bytes the value in bytes
     * @param unit  the target unit (B, KB, MB, GB, TB)
     * @return the formatted string
     */
    private String formatByteSize(long bytes, String unit) {
        long multiplier = getMultiplier(unit);
        double value = (double) bytes / multiplier;
        return String.format("%.0f%s", value, unit);
    }

    /**
     * Parses a byte size string into bytes
     * 
     * @param value the byte size string (e.g., "10KB", "1.5MB")
     * @return the value in bytes
     * @throws IllegalArgumentException if the format is invalid
     */
    private long parseByteSize(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Byte size value cannot be null or empty");
        }

        value = value.trim().toUpperCase();
        int unitIndex = -1;
        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i)) && value.charAt(i) != '.') {
                unitIndex = i;
                break;
            }
        }

        if (unitIndex == -1) {
            return Long.parseLong(value);
        }

        String numberStr = value.substring(0, unitIndex);
        String unit = value.substring(unitIndex);

        double number = Double.parseDouble(numberStr);
        long multiplier = getMultiplier(unit);

        return Math.round(number * multiplier);
    }

    /**
     * Gets the multiplier for a byte size unit
     * 
     * @param unit the unit (B, KB, MB, GB, TB)
     * @return the multiplier in bytes
     * @throws IllegalArgumentException if the unit is not supported
     */
    private long getMultiplier(String unit) {
        switch (unit) {
            case "B":
                return 1L;
            case "KB":
                return 1024L;
            case "MB":
                return 1024L * 1024L;
            case "GB":
                return 1024L * 1024L * 1024L;
            case "TB":
                return 1024L * 1024L * 1024L * 1024L;
            default:
                throw new IllegalArgumentException("Unsupported byte size unit: " + unit);
        }
    }
}