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
 * A token representing a time duration (e.g., "5s", "10m") with its equivalent in nanoseconds.
 */
public class TimeDuration implements Token {
  private final String value;
  private final long nanoseconds;

  /**
   * Constructs a {@code TimeDuration} token from a string representation.
   *
   * @param value the string representation of the duration (e.g., "5s")
   */

  public TimeDuration(String value) {
    this.value = value;
    this.nanoseconds = parseNanoseconds(value);
  }

  /**
   * Returns the string value of the duration.
   *
   * @return the string value
   */
  @Override
  public String value() {
    return value;
  }

  /**
   * Returns the token type as {@code TIME_DURATION}.
   *
   * @return the token type
   */
  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  /**
   * Converts the duration to a JSON object containing the value and nanoseconds.
   *
   * @return a {@code JsonElement} representing the token
   */
  @Override
  public JsonElement toJson() {
    JsonObject json = new JsonObject();
    json.addProperty("value", value);
    json.addProperty("nanoseconds", nanoseconds);
    return json;
  }

  /**
   * Returns the duration in nanoseconds.
   *
   * @return the number of nanoseconds
   */
  public long getNanoseconds() {
    return nanoseconds;
  }

  /**
   * Parses a string representation of a duration into nanoseconds.
   *
   * @param input the string to parse (e.g., "5s")
   * @return the equivalent number of nanoseconds
   */

  private long parseNanoseconds(String input) {
    String normalized = input.toLowerCase();
    double number = Double.parseDouble(normalized.replaceAll("[a-z]+", ""));
    String unit = normalized.replaceAll("[0-9.]+", "");
    switch (unit) {
      case "ms":
        return (long) (number * 1_000_000);
      case "s":
        return (long) (number * 1_000_000_000);
      case "m":
        return (long) (number * 60 * 1_000_000_000);
      case "h":
        return (long) (number * 3600 * 1_000_000_000);
      case "d":
        return (long) (number * 24 * 3600 * 1_000_000_000);
      case "ns":
      default:
        return (long) number;
    }

  }

}


