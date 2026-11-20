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

import java.util.Locale;

public class TimeDuration implements Token {
    private final long millis;

    public TimeDuration(String value) {
        this.millis = parseTimeDuration(value);
    }
    //calculate canonical value, ignoring the fractions for now
    private long parseTimeDuration(String value) {
        String val = value.toLowerCase(Locale.ROOT).trim();
        if (val.endsWith("ms")) {
            return Long.parseLong(val.replace("ms", "").trim());
        } else if (val.endsWith("s")) {
            return Long.parseLong(val.replace("s", "").trim()) * 1000L;
        } else if (val.endsWith("m")) {
            return Long.parseLong(val.replace("m", "").trim()) * 60L * 1000L;
        } else if (val.endsWith("h")) {
            return Long.parseLong(val.replace("h", "").trim()) * 3600L * 1000L;
        } else if (val.endsWith("d")) {
            return Long.parseLong(val.replace("d", "").trim()) * 24L * 3600L * 1000L;
        } else {
            throw new IllegalArgumentException("Invalid time duration format: " + value);
        }
    }

    public long getMillis() {
        return millis;
    }
    //implement interface
    @Override
    public Object value() {
        return null;
    }

    @Override
    public TokenType type() {
        return null;
    }

    @Override
    public JsonElement toJson() {
        return null;
    }
}
