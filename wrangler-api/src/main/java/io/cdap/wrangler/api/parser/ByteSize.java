/*
 * Copyright © 2017-2025 Cask Data, Inc.
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

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A token representing a byte size value (e.g., "10KB", "1.5MB").
 */
@PublicEvolving
public class ByteSize implements Token {
    private final String value;
    private final long bytes;
    private static final Pattern PATTERN = Pattern.compile("^(\\d+\\.?\\d*)\\s*([a-zA-Z]+)$");
    private static final long KB = 1024L;
    private static final long MB = KB * 1024L;
    private static final long GB = MB * 1024L;
    private static final long TB = GB * 1024L;
    private static final long PB = TB * 1024L;

    public ByteSize(String value) {
        this.value = value;
        this.bytes = parseBytes(value);
    }

    /**
     * Parses the input string to compute the size in bytes.
     *
     * @param input the byte size string (e.g., "10KB")
     * @return the size in bytes
     * @throws IllegalArgumentException if the input is invalid
     */
    private long parseBytes(String input) {
        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException("Byte size cannot be null or empty");
        }
        Matcher matcher = PATTERN.matcher(input.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid byte size format: " + input);
        }

        BigDecimal number;
        try {
            number = new BigDecimal(matcher.group(1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number format in byte size: " + matcher.group(1), e);
        }
        String unit = matcher.group(2).toLowerCase();

        switch (unit) {
            case "b":
                return number.longValueExact();
            case "kb":
                return number.multiply(new BigDecimal(KB)).longValueExact();
            case "mb":
                return number.multiply(new BigDecimal(MB)).longValueExact();
            case "gb":
                return number.multiply(new BigDecimal(GB)).longValueExact();
            case "tb":
                return number.multiply(new BigDecimal(TB)).longValueExact();
            case "pb":
                return number.multiply(new BigDecimal(PB)).longValueExact();
            default:
                throw new IllegalArgumentException("Unknown byte unit: " + unit);
        }
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
        object.addProperty("bytes", bytes);
        return object;
    }

    /**
     * Returns the size in bytes.
     *
     * @return the size in bytes
     */
    public long getBytes() {
        return bytes;
    }
}