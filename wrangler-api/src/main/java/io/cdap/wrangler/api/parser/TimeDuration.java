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
 * A class representing time duration values with units (e.g., "10ms", "1.5s").
 * This token type is used for parsing time intervals in various units.
 */
@PublicEvolving
public class TimeDuration implements Token {
  /** Number of milliseconds in a second. */
  private static final long MS_PER_SECOND = 1000L;
  
  /** Number of milliseconds in a minute. */
  private static final long MS_PER_MINUTE = 60L * MS_PER_SECOND;
  
  /** Number of milliseconds in an hour. */
  private static final long MS_PER_HOUR = 60L * MS_PER_MINUTE;
  
  /** Number of milliseconds in a day. */
  private static final long MS_PER_DAY = 24L * MS_PER_HOUR;

  /** The duration in milliseconds. */
  private final long milliseconds;

  /** The original input string. */
  private final String originalInput;

  /**
   * Creates a new TimeDuration instance from a string representation.
   *
   * @param input The input string (e.g. "10ms", "1.5s")
   * @throws IllegalArgumentException if the input format is invalid
   */
  public TimeDuration(String input) {
    this.originalInput = input;
    this.milliseconds = parseTime(input);
  }

  /**
   * Parses the time string into milliseconds.
   *
   * @param time The time string to parse
   * @return The number of milliseconds
   * @throws IllegalArgumentException if the unit is unknown
   */
  private long parseTime(final String time) {
    String number = time.replaceAll("[^0-9.]", "");
    String unit = time.replaceAll("[0-9.]", "");
    double value = Double.parseDouble(number);
    
    switch (unit.toLowerCase()) {
      case "ms":
        return (long) value;
      case "s":
        return (long) (value * MS_PER_SECOND);
      case "m":
        return (long) (value * MS_PER_MINUTE);
      case "h":
        return (long) (value * MS_PER_HOUR);
      case "d":
        return (long) (value * MS_PER_DAY);
      default:
        throw new IllegalArgumentException("Unknown time unit: " + unit);
    }
  }

  /**
   * Gets the duration in milliseconds.
   *
   * @return The number of milliseconds
   */
  public long getMilliseconds() {
    return milliseconds;
  }

  /**
   * Gets the duration in seconds.
   *
   * @return The number of seconds
   */
  public double getSeconds() {
    return milliseconds / (double) MS_PER_SECOND;
  }

  /**
   * Gets the duration in minutes.
   *
   * @return The number of minutes
   */
  public double getMinutes() {
    return milliseconds / (double) MS_PER_MINUTE;
  }

  /**
   * Gets the duration in hours.
   *
   * @return The number of hours
   */
  public double getHours() {
    return milliseconds / (double) MS_PER_HOUR;
  }

  /**
   * Gets the duration in days.
   *
   * @return The number of days
   */
  public double getDays() {
    return milliseconds / (double) MS_PER_DAY;
  }

  @Override
  public String value() {
    return originalInput;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.TIME_DURATION.name());
    object.addProperty("value", milliseconds);
    object.addProperty("originalInput", originalInput);
    return object;
  }
}
