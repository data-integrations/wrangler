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
 * Represents a ByteSize object that can parse and store byte size values
 * in different units such as KB, MB, GB, or TB.
 */
@PublicEvolving
public class ByteSize implements Token {
    private long bytes;

    // Regular expression pattern to match byte sizes with optional units (KB, MB,
    // GB, TB)
    private static final Pattern BYTE_SIZE_PATTERN = Pattern.compile("^\\s*(\\d+)\\s*(KB|MB|GB|TB)?\\s*$",
            Pattern.CASE_INSENSITIVE);

    private static final int KILOBYTE = 1024;
    private static final int MEGABYTE = KILOBYTE * KILOBYTE;
    private static final int GIGABYTE = KILOBYTE * MEGABYTE;
    private static final int TERABYTE = KILOBYTE * GIGABYTE;

    /**
     * Constructor to initialize the ByteSize object by parsing the input token
     * string.
     *
     * @param token The token representing byte size (e.g., "10KB", "100MB").
     * @throws IllegalArgumentException if the token does not match the expected
     *                                  format.
     */
    public ByteSize(String token) {
        Matcher matcher = BYTE_SIZE_PATTERN.matcher(token);
        if (matcher.matches()) {
            long value = Long.parseLong(matcher.group(1));
            String unit = matcher.group(2);
            switch (unit == null ? "" : unit.toUpperCase()) {
                case "KB":
                    this.bytes = value * KILOBYTE;
                    break;
                case "MB":
                    this.bytes = value * MEGABYTE;
                    break;
                case "GB":
                    this.bytes = value * GIGABYTE;
                    break;
                case "TB":
                    this.bytes = value * TERABYTE;
                    break;
                default:
                    this.bytes = value; // bytes if no unit is specified
            }
        } else {
            throw new IllegalArgumentException("Invalid ByteSize format: " + token);
        }
    }

    /**
     * Returns the value of the byte size.
     *
     * @return The byte size as a Long object.
     */
    @Override
    public Long value() {
        return Long.valueOf(bytes); // Return as Long object to match the Token interface
    }

    /**
     * Returns the type of the token (ByteSize).
     *
     * @return The TokenType for ByteSize.
     */
    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    /**
     * Converts the ByteSize object to its JSON representation.
     *
     * @return The JSON representation of the ByteSize object.
     */
    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", TokenType.BYTE_SIZE.name());
        object.addProperty("value", bytes);
        return object;
    }

    /**
     * Gets the byte size in bytes.
     *
     * @return The byte size in bytes.
     */
    public long getBytes() {
        return bytes;
    }
}

