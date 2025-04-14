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

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A token that represents a time duration value with a unit suffix (e.g., "150ms", "2h").
 * The value is stored in its canonical form (total nanoseconds).
 */
@PublicEvolving
public class TimeDuration implements Token, Serializable {
  private static final long serialVersionUID = 1L;
  private static final Pattern TIME_DURATION_PATTERN = Pattern.compile(
    "^([0-9]+(?:\\.[0-9]+)?)(ns|us|ms|s|m|h|d)$", 
    Pattern.CASE_INSENSITIVE
  );
  
  private final long nanoseconds;
  private final String originalValue;

  /**
   * Creates a new TimeDuration token from a string representation.
   *
   * @param value The string representation of the time duration (e.g., "150ms", "2h")
   * @throws IllegalArgumentException if the value cannot be parsed
   */
  public TimeDuration(String value) {
    this.originalValue = value;
    Matcher matcher = TIME_DURATION_PATTERN.matcher(value);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid time duration format: " + value);
    }

    double number = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(2).toLowerCase();

    switch (unit) {
      case "ns":
        nanoseconds = (long) number;
        break;
      case "us":
        nanoseconds = (long) (number * 1000);
        break;
      case "ms":
        nanoseconds = (long) (number * 1000 * 1000);
        break;
      case "s":
        nanoseconds = (long) (number * 1000 * 1000 * 1000);
        break;
      case "m":
        nanoseconds = (long) (number * 60 * 1000 * 1000 * 1000);
        break;
      case "h":
        nanoseconds = (long) (number * 60 * 60 * 1000 * 1000 * 1000);
        break;
      case "d":
        nanoseconds = (long) (number * 24 * 60 * 60 * 1000 * 1000 * 1000);
        break;
      default:
        throw new IllegalArgumentException("Unsupported time unit: " + unit);
    }
  }

  /**
   * Returns the time duration in its canonical form (total nanoseconds).
   *
   * @return The total number of nanoseconds
   */
  public long getNanoseconds() {
    return nanoseconds;
  }

  @Override
  public Object value() {
    return nanoseconds;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", type().name());
    object.addProperty("value", nanoseconds);
    object.addProperty("original", originalValue);
    return object;
  }

  @Override
  public String toString() {
    return originalValue;
  }
} 
