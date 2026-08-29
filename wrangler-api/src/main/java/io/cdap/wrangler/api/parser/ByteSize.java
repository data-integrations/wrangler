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
/**
 * Token for representing byte sizes like "10KB", "1MB", etc.
 */

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A token representing a byte size value like "10KB", "1MB", etc.
 */
public class ByteSize implements Token {
    private static final Pattern PATTERN = Pattern.compile("(?i)(\\d+(\\.\\d+)?)([KMGTPEZY]?B)");

    private final String original;
    private final long bytes;

    /**
     * Constructs a ByteSize token by parsing the given string.
     *
     * @param value the byte size string (e.g., "10KB", "1MB")
     * @throws IllegalArgumentException if the format is invalid
     */
    public ByteSize(String value) {
        this.original = value;

        Matcher matcher = PATTERN.matcher(value.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid byte size: " + value);
        }

        double number = Double.parseDouble(matcher.group(1));
        String unit = matcher.group(3).toUpperCase();

        switch (unit) {
            case "B":
                bytes = (long) number;
                break;
            case "KB":
                bytes = (long) (number * 1024);
                break;
            case "MB":
                bytes = (long) (number * 1024 * 1024);
                break;
            case "GB":
                bytes = (long) (number * 1024 * 1024 * 1024);
                break;
            case "TB":
                bytes = (long) (number * 1024L * 1024 * 1024 * 1024);
                break;
            case "PB":
                bytes = (long) (number * 1024L * 1024 * 1024 * 1024 * 1024);
                break;
            case "EB":
                bytes = (long) (number * 1024L * 1024 * 1024 * 1024 * 1024 * 1024);
                break;
            case "ZB":
                bytes = (long) (number * 1024L * 1024 * 1024 * 1024 * 1024 * 1024 * 1024);
                break;
            case "YB":
                bytes = (long) (number * 1024L * 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024);
                break;
            default:
                throw new IllegalArgumentException("Unknown byte unit: " + unit);
        }
    }

    /**
     * Returns the size in bytes.
     *
     * @return the size in bytes
     */
    public long getBytes() {
        return bytes;
    }

    @Override
    public Object value() {
        return original;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(original);
    }
}
