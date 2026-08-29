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
 * Represents a token that holds time duration values like "150ms", "2.5s", etc.
 * This class extends the Token base class.
 */
public class TimeDuration implements Token {
 
    // Stores the parsed value in milliseconds (canonical unit)
    private long milliseconds;
    private final String originalValue;
 
    /**
     * Constructor to initialize the TimeDuration token with a value. ("150ms","2.5s")
     */
    public TimeDuration(String value) {
        this.milliseconds = parseDuration(value); // Parse and convert to milliseconds
        this.originalValue = value;
    }
 
    /**
     * Parses the time duration string and converts it into milliseconds.
     */
    private long parseDuration(String value) {
        value = value.toLowerCase();
        if (value.endsWith("ns")) {
            // Convert nanoseconds to milliseconds
            return Long.parseLong(value.replace("ns", "")) / 1000000;
        }   else if (value.endsWith("us")) {
            // Convert microseconds to milliseconds
            return Long.parseLong(value.replace("us", "")) / 1000;
        }   else if (value.endsWith("ms")) {
            // Already in milliseconds
            return Long.parseLong(value.replace("ms", ""));
        }   else if (value.endsWith("s")) {
            // Convert seconds to milliseconds
            return (long) (Double.parseDouble(value.replace("s", "")) * 1000);
        }   else if (value.endsWith("m")) {
            // Convert minutes to milliseconds
            return (long) (Double.parseDouble(value.replace("m", "")) * 60 * 1000);
        }   else if (value.endsWith("h")) {
            // Convert hours to milliseconds
            return (long) (Double.parseDouble(value.replace("h", "")) * 60 * 60 * 1000);
        }   else {
            throw new IllegalArgumentException("Invalid time duration format: " + value);
        }
    }
    
    // Canonical unit method
    public long getMilliseconds() {
        return milliseconds;
    }
    
    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(originalValue); 
    }
    
    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }
    
    /**
     * Returns a string representation of the TimeDuration token.
     */
    @Override
    public String value() {
        return "TimeDuration{" + "value='" + originalValue + '\'' + '}';
    }
}
