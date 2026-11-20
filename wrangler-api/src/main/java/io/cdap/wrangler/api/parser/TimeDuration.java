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
public class TimeDuration implements Token {

    private final long milliSec;

    public TimeDuration(String value) {
        // Validate and parse the input string
        this.milliSec = parseTime(value);
    }

    private long parseTime(String value) {
        // Trim any spaces around the value for robustness
        value = value.trim();

        // Check for empty values
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Time duration string cannot be empty.");
        }

        // Regex to separate the numeric part and the unit part
        String numberPart = value.replaceAll("[^0-9.-]", ""); // Allow negative and decimal points
        String unitPart = value.replaceAll("[0-9.-]", ""); // Extract units (e.g., ms, s)

        // Check if both number and unit parts are found
        if (numberPart.isEmpty() || unitPart.isEmpty()) {
            throw new IllegalArgumentException("Invalid time duration format: " + value);
        }

        // Handle floating-point numbers if necessary
        double numericValue = Double.parseDouble(numberPart);

        // Handle negative values (optional, depending on your use case)
        boolean isNegative = numericValue < 0;

        // Convert units to milliseconds
        long resultInMillis = 0;
        switch (unitPart) {
            case "ms":
                resultInMillis = (long) numericValue;
                break;
            case "s":
                resultInMillis = (long) (numericValue * 1000L);
                break;
            case "m":
                resultInMillis = (long) (numericValue * 60L * 1000L);
                break;
            case "h":
                resultInMillis = (long) (numericValue * 60L * 60L * 1000L);
                break;
            case "d":
                resultInMillis = (long) (numericValue * 60L * 60L * 24L * 1000L);
                break;
            default:
                throw new IllegalArgumentException("Unknown time unit: " + unitPart);
        }

        // If the value was negative, return the negative result
        return isNegative ? -resultInMillis : resultInMillis;
    }

    @Override
    public Long value() {
        return milliSec;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", TokenType.TIME_DURATION.name());
        object.addProperty("value", milliSec);
        return object;
    }
}
