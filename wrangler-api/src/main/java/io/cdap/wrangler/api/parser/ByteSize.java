/*
 * Copyright © 2024 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
    private final String original;
    private final long bytes;

    public ByteSize(String token) {
        this.original = token;
        this.bytes = parseByteSize(token);
    }

    private long parseByteSize(String input) {
        input = input.trim().toUpperCase();
        if (input.endsWith("KB")) return Long.parseLong(input.replace("KB", "")) * 1024;
        if (input.endsWith("MB")) return Long.parseLong(input.replace("MB", "")) * 1024 * 1024;
        if (input.endsWith("GB")) return Long.parseLong(input.replace("GB", "")) * 1024 * 1024 * 1024;
        if (input.endsWith("B")) return Long.parseLong(input.replace("B", ""));
        throw new IllegalArgumentException("Invalid byte size format: " + input);
    }

    @Override
    public Object value() {
        return bytes;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    public long getBytes() {
        return bytes;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(bytes);
    }
}