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
 * Represents a token that holds byte size values like "10KB", "1.5MB", etc.
 * This class extends the Token base class.
 */
public class ByteSize implements Token {
    // Stores the parsed value in bytes (canonical unit)
    private long bytes;
    private final String originalValue;
 
    /**
     * Constructor to initialize the ByteSize token with a value. ("10KB","1.5MB")
     */
    public ByteSize(String value) {
        this.bytes = parseBytes(value); // Convert string to bytes
        this.originalValue = value;
    }
 
    private long parseBytes(String value) {
        value = value.toUpperCase();
        if (value.endsWith("KB")) {
            // Convert kilobytes to bytes
            return (long) (Double.parseDouble(value.replace("KB", "")) * 1024);
        }   else if (value.endsWith("MB")) {
            // Convert megabytes to bytes
            return (long) (Double.parseDouble(value.replace("MB", "")) * 1024 * 1024);
        }   else if (value.endsWith("GB")) {
            // Convert gigabytes to bytes
            return (long) (Double.parseDouble(value.replace("GB", "")) * 1024 * 1024 * 1024);
        }   else if (value.endsWith("TB")) {
            // Convert terabytes to bytes
            return (long) (Double.parseDouble(value.replace("TB", "")) * 1024L * 1024L * 1024L * 1024L);
        }   else if (value.endsWith("B")) {
            // Bytes are already in the base unit
            return (long) Double.parseDouble(value.replace("B", ""));
        }   else {
            throw new IllegalArgumentException("Invalid byte size format: " + value);
        }
    }
    
    public long getBytes() {
        return bytes;
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
        * Returns a string representation of the ByteSize token.
    */
    @Override
    public String value() {
        return "ByteSize{" + "value='" + originalValue + '\'' + '}';
    }
}
