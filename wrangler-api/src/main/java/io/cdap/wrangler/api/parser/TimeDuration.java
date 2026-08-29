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
    private final String value;

    public TimeDuration(String value) {
        this.value = value;
    }

    private long getTimeDuration(String value) {
        // Extract the numeric part and the unit part.
        // Allow decimals in the numeric portion (using [0-9\\.]+).
        String numberPart = value.replaceAll("[A-Za-z]+", "");
        String unitPart = value.replaceAll("[0-9\\.]+", "").toLowerCase();

        // Parse the numeric part as a double to support decimal values.
        double parsedValue = Double.parseDouble(numberPart);
        double result;

        switch (unitPart) {
            case "ms":
                result = parsedValue;
                break;
            case "s":
                result = parsedValue * 1000L;
                break;
            case "m":
                result = parsedValue * 60 * 1000L;
                break;
            case "h":
                result = parsedValue * 60 * 60 * 1000L;
                break;
            case "d":
                result = parsedValue * 24 * 60 * 60 * 1000L;
                break;
            default:
                throw new IllegalArgumentException("Unknown time unit in: " + unitPart);
        }
        return (long) result;
    }

    public long getMilliseconds() {
        return getTimeDuration(value);
    }

    @Override
    public String value() {
        return value;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", TokenType.TIME_DURATION.name());
        object.addProperty("value", value);

        return object;
    }
}
