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
import io.cdap.wrangler.api.annotations.PublicEvolving;

@PublicEvolving
public class ByteSize implements Token {
    private final String value;

    public ByteSize(String value) {
        this.value = value;
    }

    private long getParsedBytes(String value) {
        // Extract numeric and unit portions.
        String numberPart = value.replaceAll("[A-Za-z]+", "");
        String unitPart = value.replaceAll("[0-9\\.]+", ""); // also allow a decimal point

        // Standardize the unit to upper case.
        unitPart = unitPart.toUpperCase();

        // Parse the numeric part as a double to allow decimals.
        double parsedValue = Double.parseDouble(numberPart);

        // Multiply according to the unit and cast to long.
        double result;
        switch (unitPart) {
            case "B":
                result = parsedValue;
                break;
            case "KB":
                result = parsedValue * 1024L;
                break;
            case "MB":
                result = parsedValue * 1024L * 1024L;
                break;
            case "GB":
                result = parsedValue * 1024L * 1024L * 1024L;
                break;
            case "TB":
                result = parsedValue * 1024L * 1024L * 1024L * 1024L;
                break;
            default:
                throw new IllegalArgumentException("Unknown byte unit: " + unitPart);
        }
        return (long) result;
    }

    public Long getBytes() {
        return getParsedBytes(value);
    }

    @Override
    public String value() {

        return value;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", TokenType.BYTE_SIZE.name());
        object.addProperty("value", value);

        return object;
    }

}
