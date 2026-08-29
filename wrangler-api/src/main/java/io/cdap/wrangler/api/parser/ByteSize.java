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
import com.google.gson.JsonPrimitive;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * This class <code>ByteSize</code> represents the byte size token as identified by the directive parser.
 * It handles parsing and conversion of byte size strings like "10KB", "1.5MB", "2GB", etc.
 */
public class ByteSize implements Token {
    // Regex pattern to match byte size strings
    private static final Pattern BYTE_SIZE_PATTERN = Pattern.compile("^(\\d+(?:\\.\\d+)?)\\s*([kKmMgGtT]?[bB])$");

    // Unit conversion factors (to bytes)
    private static final Map<String, Long> UNIT_MULTIPLIERS = new HashMap<>();
    static {
        UNIT_MULTIPLIERS.put("b", 1L);
        UNIT_MULTIPLIERS.put("B", 1L);
        UNIT_MULTIPLIERS.put("kb", 1024L);
        UNIT_MULTIPLIERS.put("KB", 1024L);
        UNIT_MULTIPLIERS.put("mb", 1024L * 1024L);
        UNIT_MULTIPLIERS.put("MB", 1024L * 1024L);
        UNIT_MULTIPLIERS.put("gb", 1024L * 1024L * 1024L);
        UNIT_MULTIPLIERS.put("GB", 1024L * 1024L * 1024L);
        UNIT_MULTIPLIERS.put("tb", 1024L * 1024L * 1024L * 1024L);
        UNIT_MULTIPLIERS.put("TB", 1024L * 1024L * 1024L * 1024L);
    }

    private final String original;   // The original byte size string
    private final double value;      // The parsed numeric value
    private final String unit;       // The unit (B, KB, MB, GB, TB)
    private final long bytes;        // The canonical value in bytes

    /**
     * Constructor for the ByteSize token.
     *
     * @param byteSizeStr The byte size string to parse (e.g. "10KB", "1.5MB")
     * @throws IllegalArgumentException If the string is not a valid byte size format
     */
    public ByteSize(String byteSizeStr) {
        this.original = byteSizeStr;

        Matcher matcher = BYTE_SIZE_PATTERN.matcher(byteSizeStr.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    String.format("Invalid byte size format: '%s'. Expected format: <number><unit> (e.g. 10MB, 1.5GB)", byteSizeStr));
        }

        this.value = Double.parseDouble(matcher.group(1));
        this.unit = matcher.group(2);

        if (!UNIT_MULTIPLIERS.containsKey(this.unit)) {
            throw new IllegalArgumentException(
                    String.format("Invalid byte size unit: '%s'. Supported units: B, KB, MB, GB, TB", this.unit));
        }

        this.bytes = Math.round(this.value * UNIT_MULTIPLIERS.get(this.unit));
    }

    /**
     * Gets the original byte size string.
     *
     * @return The original byte size string
     */
    public String getOriginal() {
        return original;
    }

    /**
     * Gets the numeric value part of the byte size.
     *
     * @return The numeric value
     */
    public double getValue() {
        return value;
    }

    /**
     * Gets the unit part of the byte size.
     *
     * @return The unit (B, KB, MB, GB, TB)
     */
    public String getUnit() {
        return unit;
    }

    /**
     * Gets the canonical value in bytes.
     *
     * @return The value in bytes
     */
    public long getBytes() {
        return bytes;
    }

    /**
     * Converts the byte value to a specified unit.
     *
     * @param targetUnit The target unit (B, KB, MB, GB, TB)
     * @return The value in the target unit
     * @throws IllegalArgumentException If the target unit is not supported
     */
    public double toUnit(String targetUnit) {
        if (!UNIT_MULTIPLIERS.containsKey(targetUnit)) {
            throw new IllegalArgumentException(
                    String.format("Invalid target unit: '%s'. Supported units: B, KB, MB, GB, TB", targetUnit));
        }

        return (double) bytes / UNIT_MULTIPLIERS.get(targetUnit);
    }

    /**
     * Formats the byte size to a readable string in the specified unit.
     *
     * @param targetUnit The target unit (B, KB, MB, GB, TB)
     * @param precision The number of decimal places to include
     * @return A formatted string representation
     * @throws IllegalArgumentException If the target unit is not supported
     */
    public String format(String targetUnit, int precision) {
        double converted = toUnit(targetUnit);

        // Format with specified precision
        BigDecimal bd = BigDecimal.valueOf(converted);
        bd = bd.setScale(precision, BigDecimal.ROUND_HALF_UP);

        return bd.doubleValue() + " " + targetUnit;
    }

    @Override
    public Object value() {
        return null;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        return null;
    }
}