/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package io.cdap.wrangler.api.parser;

import com.google.gson.JsonPrimitive;

import java.util.Locale;

/**
 * Token representing a byte size (e.g., 10MB, 512KB, 2GB).
 */
public class ByteSize implements Token {
    private final long bytes;
    private final String value;

    public ByteSize(String value) {
        this.value = value.trim().toUpperCase(Locale.ROOT);
        this.bytes = parseByteSize(this.value);
    }

    private long parseByteSize(String str) {
        if (str.endsWith("KB")) {
            return (long) (Double.parseDouble(str.replace("KB", "")) * 1024);
        } else if (str.endsWith("MB")) {
            return (long) (Double.parseDouble(str.replace("MB", "")) * 1024 * 1024);
        } else if (str.endsWith("GB")) {
            return (long) (Double.parseDouble(str.replace("GB", "")) * 1024 * 1024 * 1024);
        } else if (str.endsWith("TB")) {
            return (long) (Double.parseDouble(str.replace("TB", "")) * 1024L * 1024 * 1024 * 1024);
        } else if (str.endsWith("PB")) {
            return (long) (Double.parseDouble(str.replace("PB", "")) * 1024L * 1024 * 1024 * 1024 * 1024);
        } else if (str.endsWith("B")) {
            return Long.parseLong(str.replace("B", ""));
        } else {
            throw new IllegalArgumentException("Unknown byte size format: " + str);
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
    public JsonPrimitive toJson() {
        return new JsonPrimitive(getBytes());
    }
}