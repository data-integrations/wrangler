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
        // Find where the digits end and unit begins
        int i = 0;
        while (i < durationStr.length() && (Character.isDigit(durationStr.charAt(i)) || durationStr.charAt(i) == '.')) {
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
        
        // If no unit specified, assume milliseconds
        if (i == durationStr.length()) {
            return (long) (number * 1_000_000); // Convert ms to ns
        }
        
        // Parse the unit part (case insensitive)
        String unit = durationStr.substring(i).toLowerCase();
        
        // Convert to nanoseconds based on unit
        switch (unit) {
            case "ns":
                return (long) number;
            case "ms":
                return (long) (number * 1_000_000); // 1 ms = 1,000,000 ns
            case "s":
                return (long) (number * 1_000_000_000); // 1 s = 1,000,000,000 ns
            case "min":
                return (long) (number * 60 * 1_000_000_000L); // 1 min = 60,000,000,000 ns
            case "h":
                return (long) (number * 60 * 60 * 1_000_000_000L); // 1 h = 3,600,000,000,000 ns
            case "d":
                return (long) (number * 24 * 60 * 60 * 1_000_000_000L); // 1 d = 86,400,000,000,000 ns
            default:
                throw new IllegalArgumentException("Unknown time unit: " + unit);
        }
    }

    @Override
    public Object value() {
        return this.nanoseconds;
    }

    @Override
    public TokenType type() {
        return TokenType.TIMEDURATION;
    }

    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", type().name());
        object.addProperty("value", nanoseconds);
        object.addProperty("original", originalText);
        return object;
    }
    
    // Convenience method to get nanoseconds directly
    public long getNanoseconds() {
        return nanoseconds;
    }
    
    // Helper methods for unit conversions
    public double getMicroseconds() {
        return nanoseconds / 1_000.0;
    }
    
    public double getMilliseconds() {
        return nanoseconds / 1_000_000.0;
    }
    
    public double getSeconds() {
        return nanoseconds / 1_000_000_000.0;
    }
    
    public double getMinutes() {
        return nanoseconds / (60.0 * 1_000_000_000.0);
    }
    
    public double getHours() {
        return nanoseconds / (60.0 * 60.0 * 1_000_000_000.0);
    }
    
    public double getDays() {
        return nanoseconds / (24.0 * 60.0 * 60.0 * 1_000_000_000.0);
    }
    
    @Override
    public String toString() {
        return originalText;
    }
}