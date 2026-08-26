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

public class TimeDuration implements Token{
    private final long millis;

    public TimeDuration(String input) {
        String valuePart = input.replaceAll("[^0-9.]", "");
        String unitPart = input.replaceAll("[0-9.]", "").toLowerCase();

        double value = Double.parseDouble(valuePart);

        switch (unitPart) {
            case "ms":
                this.millis = (long) value;
                break;
            case "s":
            case "sec":
                this.millis = (long) (value * 1000);
                break;
            case "m":
            case "min":
                this.millis = (long) (value * 60 * 1000);
                break;
            case "h":
                this.millis = (long) (value * 60 * 60 * 1000);
                break;
            case "d":
                this.millis = (long) (value * 24 * 60 * 60 * 1000);
                break;
            default:
                throw new IllegalArgumentException("Unknown time unit: " + unitPart);
        }
    }

    public long getMilliseconds() {
        return millis;
    }

    @Override
    public Object value() {
        return millis;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(millis);
    }

}
