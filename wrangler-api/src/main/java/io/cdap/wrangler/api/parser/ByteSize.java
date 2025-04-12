/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents a byte size token parsed from a directive argument, such as "10KB", "1.5MB", etc.
 * This class supports common units like B, KB, MB, GB, and TB, and converts them to canonical
 * form in bytes.
 *
 * <p>It implements {@link Token} so that it can be used as a typed argument in directive definitions.
 *
 * <p>If the unit is invalid or the value is malformed, an {@link IllegalArgumentException} is thrown.
 *
 * <p>Examples of valid inputs: "10KB", "2MB", "1.5GB", "100B"</p>
 *
 * @see Token
 * @see TokenType#BYTE_SIZE
 */
@PublicEvolving
public class ByteSize implements Token {

    private static final Set<String> VALID_UNITS = new HashSet<>(Arrays.asList("B", "KB", "MB", "GB", "TB"));

    private final double value;
    private final String unit;

    /**
     * Constructs a {@code ByteSize} token from the given string input.
     *
     * @param token the input string containing a numeric value followed by a valid unit (e.g. "10MB")
     * @throws IllegalArgumentException if the unit is unsupported or the numeric part is invalid
     */
    public ByteSize(String token) {
        token = token.trim().toUpperCase();
        // Extract unit by removing digits and dots.
        String extractedUnit = token.replaceAll("[0-9.]", "");
        // If no unit is provided, default to "B"
        this.unit = extractedUnit.isEmpty() ? "B" : extractedUnit;

        if (!VALID_UNITS.contains(unit)) {
            throw new IllegalArgumentException("Invalid byte size unit: " + unit);
        }

        try {
            this.value = Double.parseDouble(token.replaceAll("[^0-9.]", ""));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value in ByteSize: " + token);
        }
    }

    /**
     * Returns the canonical size in bytes.
     *
     * @return the byte size as a long
     */
    public long getBytes() {
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
                return (long) (value * 1024L * 1024 * 1024 * 1024);
            default:
                throw new IllegalStateException("Unhandled byte size unit: " + unit);
        }
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
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("type", TokenType.BYTE_SIZE.name());
        jsonObject.addProperty("value", value);
        return jsonObject;
    }
}
