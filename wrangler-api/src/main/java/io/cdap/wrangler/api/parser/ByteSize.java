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
 * The ByteSize class wraps byte size values with unit conversion capabilities.
 * An object of type {@code ByteSize} contains the value in bytes as well as
 * the original string representation.
 */
@PublicEvolving
public class ByteSize implements Token {
    private final long bytes;
    private final String originalValue;

    public ByteSize(String value) {
        this.originalValue = value;
        this.bytes = parseBytes(value);
    }

    private long parseBytes(String value) {
        String trimmed = value.trim();
        int lastDigitIndex = -1;
        for (int i = 0; i < trimmed.length(); i++) {
            if (!Character.isDigit(trimmed.charAt(i)) && trimmed.charAt(i) != '.') {
                lastDigitIndex = i;
                break;
            }
        }
        if (lastDigitIndex == -1) {
            throw new IllegalArgumentException("Invalid byte size format: " + value);
        }

        double number = Double.parseDouble(trimmed.substring(0, lastDigitIndex));
        String unit = trimmed.substring(lastDigitIndex).trim().toUpperCase();

        switch (unit) {
            case "B":
                return (long) number;
            case "KB":
                return (long) (number * 1000);
            case "MB":
                return (long) (number * 1000 * 1000);
            case "GB":
                return (long) (number * 1000 * 1000 * 1000);
            case "TB":
                return (long) (number * 1000 * 1000 * 1000 * 1000);
            case "PB":
                return (long) (number * 1000 * 1000 * 1000 * 1000 * 1000);
            case "KIB":
                return (long) (number * 1024);
            case "MIB":
                return (long) (number * 1024 * 1024);
            case "GIB":
                return (long) (number * 1024 * 1024 * 1024);
            case "TIB":
                return (long) (number * 1024 * 1024 * 1024 * 1024);
            case "PIB":
                return (long) (number * 1024 * 1024 * 1024 * 1024 * 1024);
            default:
                throw new IllegalArgumentException("Unknown byte size unit: " + unit);
        }
    }

    public long getBytes() {
        return bytes;
    }

    public double toMegabytes() {
        return bytes / (1000.0 * 1000.0);
    }

    @Override
    public String value() {
        return originalValue;
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

    public double getKB() {
        return bytes / 1000.0;
    }

    public double getMB() {
        return bytes / (1000.0 * 1000);
    }

    public double getGB() {
        return bytes / (1000.0 * 1000 * 1000);
    }

    public double getTB() {
        return bytes / (1000.0 * 1000 * 1000 * 1000);
    }

    public double getPB() {
        return bytes / (1000.0 * 1000 * 1000 * 1000 * 1000);
    }
} 

