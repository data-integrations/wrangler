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

// Ensure that the Token class is available in your project (this should be
// provided by Wrangler)
public class ByteSize implements Token {
    private final long bytes;

    public ByteSize(String rawValue) {
        // super(rawValue);
        String input = rawValue.trim();
        if (input.isEmpty()) {
            throw new IllegalArgumentException("ByteSize value cannot be empty");
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
            throw new IllegalArgumentException("Invalid numeric value in ByteSize: " + rawValue);
        }

        if (unitPart.equals("b") || unitPart.isEmpty()) {
            this.bytes = (long) numericValue;
        } else if (unitPart.equals("kb")) {
            this.bytes = (long) (numericValue * 1024);
        } else if (unitPart.equals("mb")) {
            this.bytes = (long) (numericValue * 1024 * 1024);
        } else if (unitPart.equals("gb")) {
            this.bytes = (long) (numericValue * 1024 * 1024 * 1024);
        } else if (unitPart.equals("tb")) {
            this.bytes = (long) (numericValue * 1024 * 1024 * 1024 * 1024);
        } else {
            throw new IllegalArgumentException("Unrecognized byte size unit: " + unitPart);
        }
    }

    /** Returns the size in bytes. */
    public long getBytes() {
        return bytes;
    }

    @Override
    public Object value() {
        return bytes;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        JsonObject obj = new JsonObject();
        obj.addProperty("type", TokenType.BYTE_SIZE.name());
        obj.addProperty("value", bytes);
        return obj;
    }

}
