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

public class TimeDuration implements Token {
    private final String original;
    private final long millis;

    public TimeDuration(String token) {
        this.original = token;
        this.millis = parseDuration(token);
    }

    private long parseDuration(String input) {
        input = input.trim().toLowerCase();
        if (input.endsWith("ms")) return Long.parseLong(input.replace("ms", ""));
        if (input.endsWith("s")) return Long.parseLong(input.replace("s", "")) * 1000;
        if (input.endsWith("m")) return Long.parseLong(input.replace("m", "")) * 60 * 1000;
        if (input.endsWith("h")) return Long.parseLong(input.replace("h", "")) * 60 * 60 * 1000;
        throw new IllegalArgumentException("Invalid time duration format: " + input);
    }

    @Override
    public Object value() {
        return millis;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    public long getMillis() {
        return millis;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(millis);
    }
}
