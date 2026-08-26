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
import com.google.gson.JsonPrimitive;
import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * A token implementation for time duration values.
 * Supports parsing of values like "500ms", "1.5s", "2h", etc.
 */
@PublicEvolving
public class TimeDuration implements Token {
  private static final long serialVersionUID = 1L;
  private final String rawValue;
  private final long nanoseconds;

  /**
   * Constructor for TimeDuration token.
   *
   * @param value String representation of time duration (e.g., "500ms", "1.5s")
   * @throws IllegalArgumentException if the value format is invalid
   */
  public TimeDuration(String value) {
    this.rawValue = value;
    this.nanoseconds = parseToNanos(value);
  }

  /**
   * Parses a string representation of time duration into nanoseconds.
   *
   * @param value String to parse
   * @return number of nanoseconds
   * @throws IllegalArgumentException if the format is invalid
   */
  private long parseToNanos(String value) {
    value = value.trim();
    String number = value.replaceAll("[^0-9.]", "");
    String unit = value.replaceAll("[0-9.]", "").toLowerCase();
    
    try {
      double amount = Double.parseDouble(number);
      switch (unit) {
        case "ms":
          return (long) (amount * 1_000_000);
        case "s":
          return (long) (amount * 1_000_000_000);
        case "m":
          return (long) (amount * 60 * 1_000_000_000L);
        case "h":
          return (long) (amount * 3600 * 1_000_000_000L);
        default:
          throw new IllegalArgumentException("Invalid time unit: " + unit);
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid duration format: " + value);
    }
  }

  @Override
  public Object value() {
    return rawValue;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(rawValue);
  }

  /**
   * Get the duration in nanoseconds.
   * @return number of nanoseconds
   */
  public long getNanoseconds() {
    return nanoseconds;
  }

  /**
   * Get the duration in milliseconds.
   * @return number of milliseconds
   */
  public double getMilliseconds() {
    return nanoseconds / 1_000_000.0;
  }

  /**
   * Get the duration in seconds.
   * @return number of seconds
   */
  public double getSeconds() {
    return nanoseconds / 1_000_000_000.0;
  }

  /**
   * Get the duration in minutes.
   * @return number of minutes
   */
  public double getMinutes() {
    return nanoseconds / (60.0 * 1_000_000_000.0);
  }

  /**
   * Get the duration in hours.
   * @return number of hours
   */
  public double getHours() {
    return nanoseconds / (3600.0 * 1_000_000_000.0);
  }
}



