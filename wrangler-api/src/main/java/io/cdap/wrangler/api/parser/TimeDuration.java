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
 * A parser token representing a byte size literal.
 * Converts a string with units (e.g., "1.5MB") to bytes.
 */

public class TimeDuration implements Token {
    private final long nanoSeconds;

    public TimeDuration(String rawValue) {
        // super(rawValue);
        String input = rawValue.trim();
        if (input.isEmpty()) {
            throw new IllegalArgumentException("TimeDuration value cannot be empty");
        }
        int idx = 0;
        int len = input.length();
        while (idx < len && (Character.isDigit(input.charAt(idx)) || input.charAt(idx) == '.')) {
            idx++;
        }
        String numberPart = input.substring(0, idx);
        String unitPart = input.substring(idx).toLowerCase();

        double numericValue;
        try {
            numericValue = Double.parseDouble(numberPart);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value in TimeDuration: " + rawValue);
        }

        switch (unitPart) {
            case "ms":
                nanoSeconds = (long) (numericValue * 1_000_000);
                break;
            case "s":
                nanoSeconds = (long) (numericValue * 1_000_000_000);
                break;
            case "min":
                nanoSeconds = (long) (numericValue * 60 * 1_000_000_000L);
                break;
            case "h":
                nanoSeconds = (long) (numericValue * 3600 * 1_000_000_000L);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized time unit: " + unitPart);
        }
    }

    /** Returns the duration in nanoseconds. */
    public long getNanoSeconds() {
        return nanoSeconds;
    }

    @Override
    public Object value() {
        return nanoSeconds;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        JsonObject obj = new JsonObject();
        obj.addProperty("type", TokenType.TIME_DURATION.name());
        obj.addProperty("value", nanoSeconds);
        return obj;
    }

    

}
