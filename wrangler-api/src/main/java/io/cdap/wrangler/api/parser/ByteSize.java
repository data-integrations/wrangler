/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.wrangler.api.parser;


import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

public class ByteSize implements Token {
    private final String originalValue;
    private final long bytes;

    public ByteSize(String value) {
        this.originalValue = value;
        String val = value.toUpperCase().replaceAll("\\s+", "");
        if (val.endsWith("KB")) {
            bytes = (long)(Double.parseDouble(val.replace("KB", "")) * 1024);
        } else if (val.endsWith("MB")) {
            bytes = (long)(Double.parseDouble(val.replace("MB", "")) * 1024 * 1024);
        } else if (val.endsWith("GB")) {
            bytes = (long)(Double.parseDouble(val.replace("GB", "")) * 1024 * 1024 * 1024);
        } else if (val.endsWith("TB")) {
            bytes = (long)(Double.parseDouble(val.replace("TB", "")) * 1024L * 1024L * 1024L * 1024L);
        } else if (val.endsWith("B")) {
            bytes = Long.parseLong(val.replace("B", ""));
        } else {
            throw new IllegalArgumentException("Invalid byte size: " + value);
        }
    }
    public long getBytes() {
        return bytes;
    }

    // Required by Token interface
    @Override
    public Object value() {
        return originalValue;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(originalValue);
    }
}
