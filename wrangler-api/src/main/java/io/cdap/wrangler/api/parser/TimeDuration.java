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
 * The TimeDuration class wraps time duration values with units (e.g. "150ms", "2.1s") in an object.
 * An object of type TimeDuration contains the value in nanoseconds as well as the type of the token this class represents.
 *
 * <p>This class provides methods to:
 * 1. Parse time duration strings with units into canonical nanoseconds
 * 2. Convert between different time units
 * 3. Retrieve the value in nanoseconds or in a specified unit
 * </p>
 */
@PublicEvolving
public class TimeDuration implements Token {
  private final long nanoseconds;
  private final String originalString;

  // Common time units and their multipliers in nanoseconds
  private static final long NS = 1L;
  private static final long MICRO = 1000L * NS;
  private static final long MILLI = 1000L * MICRO;
  private static final long SECOND = 1000L * MILLI;
  private static final long MINUTE = 60L * SECOND;
  private static final long HOUR = 60L * MINUTE;
  private static final long DAY = 24L * HOUR;

  public TimeDuration(String value) {
    this.originalString = value;
    this.nanoseconds = parseTimeDuration(value);
  }

  private long parseTimeDuration(String value) {
    value = value.trim().toLowerCase();
    if (value.isEmpty()) {
      return 0L;
    }

    // Extract the numeric part and unit
    int unitIndex = 0;
    while (unitIndex < value.length() && (Character.isDigit(value.charAt(unitIndex)) || value.charAt(unitIndex) == '.')) {
      unitIndex++;
    }

    if (unitIndex == 0) {
      throw new IllegalArgumentException("Invalid time duration format: " + value);
    }

    double number = Double.parseDouble(value.substring(0, unitIndex));
    String unit = value.substring(unitIndex).trim();

    // Convert to nanoseconds based on unit
    switch (unit) {
      case "ns":
        return (long) number;
      case "us":
      case "µs":
        return (long) (number * MICRO);
      case "ms":
        return (long) (number * MILLI);
      case "s":
        return (long) (number * SECOND);
      case "m":
        return (long) (number * MINUTE);
      case "h":
        return (long) (number * HOUR);
      case "d":
        return (long) (number * DAY);
      default:
        throw new IllegalArgumentException("Unsupported time unit: " + unit);
    }
  }

  /**
   * Returns the value in nanoseconds.
   *
   * @return the value in nanoseconds
   */
  @Override
  public Long value() {
    return nanoseconds;
  }

  /**
   * Returns the type of this TimeDuration object as a TokenType enum.
   *
   * @return the enumerated TokenType of this object
   */
  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  /**
   * Returns the members of this TimeDuration object as a JsonElement.
   *
   * @return Json representation of this TimeDuration object as JsonElement
   */
  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.TIME_DURATION.name());
    object.addProperty("value", originalString);
    object.addProperty("nanoseconds", nanoseconds);
    return object;
  }

  /**
   * Gets the value in a specified unit.
   *
   * @param unit the unit to convert to (ns, us, ms, s, m, h, d)
   * @return the value in the specified unit
   * @throws IllegalArgumentException if an unsupported unit is specified
   */
  public double getValue(String unit) {
    unit = unit.toLowerCase();
    switch (unit) {
      case "ns":
        return nanoseconds;
      case "us":
      case "µs":
        return (double) nanoseconds / MICRO;
      case "ms":
        return (double) nanoseconds / MILLI;
      case "s":
        return (double) nanoseconds / SECOND;
      case "m":
        return (double) nanoseconds / MINUTE;
      case "h":
        return (double) nanoseconds / HOUR;
      case "d":
        return (double) nanoseconds / DAY;
      default:
        throw new IllegalArgumentException("Unsupported time unit: " + unit);
    }
  }

  /**
   * Gets the original string representation of the time duration.
   *
   * @return the original string representation
   */
  public String getOriginalString() {
    return originalString;
  }
} 