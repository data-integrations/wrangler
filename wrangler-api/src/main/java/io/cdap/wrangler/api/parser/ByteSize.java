/*
 * Copyright © [2025] [Nitin]
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
import com.google.gson.JsonObject;

import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ByteSize implements Token {
    private static final Pattern PATTERN = Pattern.compile("(?i)(\\d+(\\.\\d+)?)([KMGTP]B?|EB|B)");
    private final double value;
    private final String unit;
    private static final DecimalFormat FORMATTER = new DecimalFormat("0.00");

    public ByteSize(String input) {
        Matcher matcher = PATTERN.matcher(input);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid byte size: " + input);
        }
        this.value = Double.parseDouble(matcher.group(1));
        this.unit = matcher.group(3).toUpperCase();
    }

    public long toBytes() {
        switch (unit) {
            case "B": return (long) value;
            case "KB": return (long) (value * 1024);
            case "MB": return (long) (value * 1024 * 1024);
            case "GB": return (long) (value * 1024 * 1024 * 1024);
            case "TB": return (long) (value * 1024L * 1024 * 1024 * 1024);
            case "PB": return (long) (value * 1024L * 1024 * 1024 * 1024 * 1024);
            case "EB": return (long) (value * 1024L * 1024 * 1024 * 1024 * 1024 * 1024);
            default: throw new IllegalStateException("Unsupported unit: " + unit);
        }
    }

    @Override
    public Object value() {
        return FORMATTER.format(value) + " " + unit;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        JsonObject obj = new JsonObject();
        obj.addProperty("type", type().name());
        obj.addProperty("value", String.valueOf(value()));
        obj.addProperty("bytes", toBytes());
        return obj;
    }

    @Override
    public String toString() {
        return value().toString();
    }
}
