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

/**
 * The TimeDuration class represents a token that includes a numeric value and a time unit (ms, s, etc.).
 * It provides functionality to parse time duration strings and convert them to canonical nanoseconds.
 */
@PublicEvolving
public class TimeDuration implements Token {
  private final double number;
  private final String unit;
  private final String value;

  public TimeDuration(String value) {
    this.value = value;
    String numberPart = value.replaceAll("[^0-9.]", "");
    String unitPart = value.replaceAll("[0-9.]", "");

    this.number = Double.parseDouble(numberPart);
    this.unit = unitPart;
  }

  /**
   * Returns the canonical duration in nanoseconds.
   *
   * @return long value representing the duration in nanoseconds
   */
  public long getNanoseconds() {
    switch (unit.toLowerCase()) {
      case "ms":
        return (long) (number * 1_000_000); // 1 millisecond = 1,000,000 nanoseconds
      case "s":
        return (long) (number * 1_000_000_000); // 1 second = 1,000,000,000 nanoseconds
      case "m":
        return (long) (number * 60 * 1_000_000_000L); // 1 minute = 60 seconds
      case "h":
        return (long) (number * 60 * 60 * 1_000_000_000L); // 1 hour = 3600 seconds
      case "d":
        return (long) (number * 24 * 60 * 60 * 1_000_000_000L); // 1 day = 86400 seconds
      default:
        throw new IllegalArgumentException("Invalid time unit: " + unit);
    }
  }

  @Override
  public Object value() {
    return value;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", type().name());
    object.addProperty("value", value);
    return object;
  }
}