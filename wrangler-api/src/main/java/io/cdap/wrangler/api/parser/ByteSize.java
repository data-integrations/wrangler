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

import java.util.Locale;

public class ByteSize implements Token {
    private final long bytes;

    public ByteSize(String value) {
        this.bytes = parseByteSize(value); // call the method
    }
    // calculate the canonical value
    private long parseByteSize(String value) {
        String val = value.toUpperCase(Locale.ROOT).trim();
        try {
            if (val.endsWith("KB")) {
                return Long.parseLong(val.replace("KB", "").trim()) * 1024L;
            } else if (val.endsWith("MB")) {
                return Long.parseLong(val.replace("MB", "").trim()) * 1024L * 1024L;
            } else if (val.endsWith("GB")) {
                return Long.parseLong(val.replace("GB", "").trim()) * 1024L * 1024L * 1024L;
            } else if (val.endsWith("TB")) {
                return Long.parseLong(val.replace("TB", "").trim()) * 1024L * 1024L * 1024L * 1024L;
            } else if (val.endsWith("PB")) {
                return Long.parseLong(val.replace("PB", "").trim()) * 1024L * 1024L * 1024L * 1024L * 1024L;
            } else if (val.endsWith("B")) {
                return Long.parseLong(val.replace("B", "").trim());
            } else {
                throw new IllegalArgumentException("Invalid byte size format: " + value);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number in byte size: " + value, e);
        }
    }

    public long getBytes() {
        return bytes;
    }
    // implement the Token interface
    @Override
    public Object value() {
        return bytes;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(bytes);
    }
}