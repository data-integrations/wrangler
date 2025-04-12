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

// ByteSize class is used to parse size like 10KB, 5MB into bytes
// and provides the value in bytes format.

/**
 * This class represents a ByteSize Parser.
 */
public class ByteSize implements Token {

    // Variable to store converted value in bytes
    private final long bytes;

    // Constructor
    // It will receive the value like "10KB" or "200MB"
    public ByteSize(String value) {
        // Parse the given value and store in bytes variable
        this.bytes = parseBytes(value);
    }

    // Method to convert input value like "10KB" to bytes
    private long parseBytes(String value) {

        // To remove extra spaces and handle small/uppercase mismatch
        value = value.trim().toUpperCase();

        // Check if empty value is passed
        if (value.length() == 0) {
            throw new IllegalArgumentException("ByteSize cannot be empty");
        }

        // Extract number part from value
        long number = Long.parseLong(value.replaceAll("[^0-9]", ""));

        // Extract unit part from value (KB, MB, GB, TB, B)
        String unit = value.replaceAll("[0-9]", "");

        // Switch case to convert value based on unit
        switch (unit) {
            case "KB":
                return number * 1024;
            case "MB":
                return number * 1024 * 1024;
            case "GB":
                return number * 1024 * 1024 * 1024;
            case "TB":
                return number * 1024L * 1024L * 1024L * 1024L;
            case "B":
                return number;
            default:
                throw new IllegalArgumentException("Invalid ByteSize format, Please Check It : " + value);
        }
    }

    // Getter method to return bytes value
    public long getBytes() {
        return this.bytes;
    }

    // Return the parsed value (required by Token interface)
    @Override
    public Object value() {
        return this.bytes;
    }

    // Return value as JSON (required by Token interface)
    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(this.bytes);
    }

    // toString() method (Optional)
    @Override
    public String toString() {
        return String.valueOf(this.bytes);
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }
}
