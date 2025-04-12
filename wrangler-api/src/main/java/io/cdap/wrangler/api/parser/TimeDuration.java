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

/**
 * This class represents a TimeDuration token.
 */

public class TimeDuration implements Token {

    // Variable to store converted value in milliseconds
    private final long milliseconds;

    public TimeDuration(String value) {
        this.milliseconds = parseMilliseconds(value);
    }

    private long parseMilliseconds(String value) {

        // To remove extra spaces and handle small/uppercase mismatch
        value = value.trim().toLowerCase();

        // Check if empty value is passed
        if (value.length() == 0) {
            throw new IllegalArgumentException("TimeDuration cannot be empty");
        }

        // Extract number part from value
        long number = Long.parseLong(value.replaceAll("[^0-9]", ""));

        // Extract unit part from value (KB, MB, GB, TB, B)
        String unit = value.replaceAll("[0-9]", "");

        switch (unit) {
            case "ms":
                return number;
            case "s":
                return number * 1000;
            case "m":
                return number * 60 * 1000;
            case "h":
                return number * 60 * 60 * 1000;
            default:
                throw new IllegalArgumentException("Invalid TimeDuration format, Please Check It : " + value);
        }
        
 

    }

    // Getter method to return milliseconds value
     public long getMilliseconds() {
        return this.milliseconds;
    }

    // Return the parsed value (required by Token interface)
    @Override
    public Object value() {
        return this.milliseconds;
    }

    // Return value as JSON (required by Token interface)
    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(this.milliseconds);
    }

    // toString() method (Optional)
    @Override
    public String toString() {
        return String.valueOf(this.milliseconds);
    }

    // Return TokenType (required by Token interface)
    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }
    
}
