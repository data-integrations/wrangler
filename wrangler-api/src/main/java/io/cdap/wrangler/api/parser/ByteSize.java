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

public class ByteSize implements Token {
    private final long bytes;
    private final String originalText;

    public ByteSize(String byteSizeStr) {
        this.originalText = byteSizeStr;
        this.bytes = parseBytes(byteSizeStr);
    }

    private long parseBytes(String byteSizeStr) {
        // Find where the digits end and unit begins
        int i = 0;
        while (i < byteSizeStr.length() && (Character.isDigit(byteSizeStr.charAt(i)) || byteSizeStr.charAt(i) == '.')) {
            i++;
        }
        
        if (i == 0) {
            throw new IllegalArgumentException("Invalid byte size format: " + byteSizeStr);
        }
        
        // Parse the number part
        double number;
        try {
            number = Double.parseDouble(byteSizeStr.substring(0, i));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid byte size number: " + byteSizeStr, e);
        }
        
        // If no unit specified, assume bytes
        if (i == byteSizeStr.length()) {
            return (long) number;
        }
        
        // Parse the unit part (case insensitive)
        String unit = byteSizeStr.substring(i).toLowerCase();
        
        // Convert to bytes based on unit
        switch (unit) {
            case "b":
                return (long) number;
            case "kb":
            case "k":
                return (long) (number * 1024);
            case "mb":
            case "m":
                return (long) (number * 1024 * 1024);
            case "gb":
            case "g":
                return (long) (number * 1024 * 1024 * 1024);
            case "tb":
            case "t":
                return (long) (number * 1024 * 1024 * 1024 * 1024);
            default:
                throw new IllegalArgumentException("Unknown byte unit: " + unit);
        }
    }

    @Override
    public Object value() {
        return this.bytes;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTESIZE;
    }

    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", type().name());
        object.addProperty("value", bytes);
        object.addProperty("original", originalText);
        return object;
    }
    
    @Override
    public String toString() {
        return originalText;
    }
}