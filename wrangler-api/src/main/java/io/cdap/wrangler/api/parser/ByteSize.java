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

/**
 * Represents a byte size token parsed from a directive expression.
 */
public class ByteSize implements Token {
    private final String value;
    private final long bytes;

    public ByteSize(String value) {
        this.value = value;
        this.bytes = parseBytes(value);
    }

    private long parseBytes(String value) {
        value = value.trim().toUpperCase();
        long multiplier = 1L;
        if (value.endsWith("KB")) {
            multiplier = 1024L;
            value = value.replace("KB", "");
        } else if (value.endsWith("MB")) {
            multiplier = 1L << 20;
            value = value.replace("MB", "");
        } else if (value.endsWith("GB")) {
            multiplier = 1L << 30;
            value = value.replace("GB", "");
        } else if (value.endsWith("TB")) {
            multiplier = 1L << 40;
            value = value.replace("TB", "");
        } else if (value.endsWith("B")) {
            value = value.replace("B", "");
        }
        try {
            return Long.parseLong(value.trim()) * multiplier;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid byte size format: " + this.value, e);
        }
    }

    public long getBytes() {
        return bytes;
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
}
