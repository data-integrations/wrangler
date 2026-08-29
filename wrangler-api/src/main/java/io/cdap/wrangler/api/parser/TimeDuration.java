/*
 * Copyright © 2024 Cask Data, Inc.
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
import io.cdap.wrangler.api.Token;
import io.cdap.wrangler.api.TokenType;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Represents a time duration token in the Wrangler grammar.
 * This class implements the Token interface and provides functionality for
 * parsing and representing time duration values with their units.
 */
public class TimeDuration implements Token {
  private final long value;
  private final TimeUnit unit;
  private final String originalString;

  /**
   * Creates a new TimeDuration token with the specified value and unit.
   *
   * @param value the numeric value of the time duration
   * @param unit the unit of the time duration (e.g., "ms", "s", "m", "h", "d")
   * @param originalString the original string representation of the time duration
   */
  public TimeDuration(long value, String unit, String originalString) {
    this.value = value;
    this.unit = parseTimeUnit(unit);
    this.originalString = originalString;
  }

  /**
   * Parses a string unit into a TimeUnit enum value.
   *
   * @param unit the unit string to parse
   * @return the corresponding TimeUnit
   * @throws IllegalArgumentException if the unit is not recognized
   */
  private TimeUnit parseTimeUnit(String unit) {
    switch (unit.toLowerCase()) {
      case "ms":
        return TimeUnit.MILLISECONDS;
      case "s":
        return TimeUnit.SECONDS;
      case "m":
        return TimeUnit.MINUTES;
      case "h":
        return TimeUnit.HOURS;
      case "d":
        return TimeUnit.DAYS;
      default:
        throw new IllegalArgumentException("Unsupported time unit: " + unit);
    }
  }

  /**
   * Returns the numeric value of the time duration.
   *
   * @return the value as a long
   */
  public long getValue() {
    return value;
  }

  /**
   * Returns the unit of the time duration.
   *
   * @return the unit as a TimeUnit
   */
  public TimeUnit getUnit() {
    return unit;
  }

  /**
   * Returns the original string representation of the time duration.
   *
   * @return the original string
   */
  public String getOriginalString() {
    return originalString;
  }

  /**
   * Converts the time duration to milliseconds.
   *
   * @return the time duration in milliseconds
   */
  public long toMillis() {
    return unit.toMillis(value);
  }

  @Override
  public Object value() {
    return toMillis();
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    JsonObject json = new JsonObject();
    json.addProperty("value", value);
    json.addProperty("unit", unit.name());
    json.addProperty("millis", toMillis());
    json.addProperty("original", originalString);
    return json;
  }

  @Override
  public String toString() {
    return originalString;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TimeDuration that = (TimeDuration) o;
    return value == that.value && unit == that.unit;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, unit);
  }
}