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

public class TimeDuration implements Token {
    private final long nanoseconds;
    private final String originalText;

    public TimeDuration(String durationStr) {
        this.originalText = durationStr;
        this.nanoseconds = parseToNanoseconds(durationStr);
    }

    private long parseToNanoseconds(String durationStr) {
        if (durationStr == null || durationStr.trim().isEmpty()) {
            throw new IllegalArgumentException("Time duration string cannot be null or empty");
        }
    
        durationStr = durationStr.trim();
        
        // Find where the digits end and unit begins
        int i = 0;
        while (i < durationStr.length() && 
               (Character.isDigit(durationStr.charAt(i)) || 
               durationStr.charAt(i) == '.' || 
               durationStr.charAt(i) == '-')) {
            i++;
        }
    
        if (i == 0) {
            throw new IllegalArgumentException("Invalid time duration format: " + durationStr);
        }
    
        // Parse the number part
        double number;
        try {
            number = Double.parseDouble(durationStr.substring(0, i));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid time duration number: " + durationStr, e);
        }
    
        // If no unit specified, assume nanoseconds
        if (i == durationStr.length()) {
            return (long) number;
        }
    
        // Parse the unit part (case insensitive)
        String unit = durationStr.substring(i).trim().toLowerCase();
    
        // Convert to nanoseconds based on unit
        switch (unit) {
            case "ns":
                return (long) number;
            case "us":
                return (long) (number * 1_000); // microseconds to nanoseconds
            case "ms":
                return (long) (number * 1_000_000); // milliseconds to nanoseconds
            case "s":
            case "sec":
            case "secs":
                return (long) (number * 1_000_000_000); // seconds to nanoseconds
            case "min":
            case "mins":
                return (long) (number * 60 * 1_000_000_000L); // minutes to nanoseconds
            case "h":
            case "hr":
            case "hrs":
                return (long) (number * 60 * 60 * 1_000_000_000L); // hours to nanoseconds
            case "d":
            case "day":
            case "days":
                return (long) (number * 24 * 60 * 60 * 1_000_000_000L); // days to nanoseconds
            default:
                throw new IllegalArgumentException("Unknown time unit: " + unit + " in duration: " + durationStr);
        }
    }
    @Override
    public Long value() {
        return this.nanoseconds;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", type().name());
        object.addProperty("value", nanoseconds);
        object.addProperty("original", originalText);
        return object;
    }

}