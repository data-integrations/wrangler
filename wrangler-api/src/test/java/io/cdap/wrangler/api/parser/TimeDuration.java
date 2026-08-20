/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

public class TimeDuration implements Token {
    private final String originalValue;
    private final long milliseconds;

    public TimeDuration(String value) {
        this.originalValue = value;
        String val = value.toLowerCase().replaceAll("\\s+", "");
        if (val.endsWith("ms")) {
            milliseconds = (long)(Double.parseDouble(val.replace("ms", "")));
        } else if (val.endsWith("s")) {
            milliseconds = (long)(Double.parseDouble(val.replace("s", "")) * 1000);
        } else if (val.endsWith("m")) {
            milliseconds = (long)(Double.parseDouble(val.replace("m", "")) * 60 * 1000);
        } else if (val.endsWith("h")) {
            milliseconds = (long)(Double.parseDouble(val.replace("h", "")) * 60 * 60 * 1000);
        } else {
            throw new IllegalArgumentException("Invalid time duration: " + value);
        }
    }

    public long getMilliseconds() {
        return milliseconds;
    }

    @Override
    public Object value() {
        return originalValue;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(originalValue);
    }
}
