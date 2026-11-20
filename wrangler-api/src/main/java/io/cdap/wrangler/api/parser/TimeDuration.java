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
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A token class representing time durations with various units (ns, ms, s, min, h, d).
 * Examples: "150ms", "2.5s", "10min"
 */
public class TimeDuration implements Token {
  private static final Pattern PATTERN = Pattern.compile(
    "^(\\d+(?:\\.\\d+)?)\\s*(ns|ms|s|min|h|hr|d|day)$", 
    Pattern.CASE_INSENSITIVE
  );
  
  private final double value;
  private final String unit;
  private final long nanoseconds;

  /**
   * Constructor to parse a time duration string.
   *
   * @param timeDuration The string representation of a time duration (e.g., "150ms", "2.5s").
   */
  public TimeDuration(String timeDuration) {
    Matcher matcher = PATTERN.matcher(timeDuration.trim());
    
    this.value = Double.parseDouble(matcher.group(1));
    this.unit = matcher.group(2).toLowerCase(Locale.ENGLISH);
    this.nanoseconds = convertToNanoseconds(this.value, this.unit);
  }
  
  /**
   * Gets the numeric value part of the time duration.
   *
   * @return The numeric value.
   */
  public Double value() {
    return value;
  }
  
  /**
   * Gets the unit part of the time duration.
   *
   * @return The unit string (e.g., "ms", "s", etc.).
   */
  public String getUnit() {
    return unit;
  }
  
  /**
   * Gets the time duration converted to nanoseconds.
   *
   * @return The duration in nanoseconds.
   */
  public long getNanoseconds() {
    return nanoseconds;
  }
  
  /**
   * Gets the time duration converted to milliseconds.
   *
   * @return The duration in milliseconds.
   */
  public double getMilliseconds() {
    return nanoseconds / 1_000_000.0;
  }
  
  /**
   * Gets the time duration converted to seconds.
   *
   * @return The duration in seconds.
   */
  public double getSeconds() {
    return nanoseconds / 1_000_000_000.0;
  }
  
  /**
   * Gets the time duration converted to minutes.
   *
   * @return The duration in minutes.
   */
  public double getMinutes() {
    return nanoseconds / (60.0 * 1_000_000_000.0);
  }
  
  /**
   * Gets the time duration converted to hours.
   *
   * @return The duration in hours.
   */
  public double getHours() {
    return nanoseconds / (3600.0 * 1_000_000_000.0);
  }
  
  /**
   * Gets the time duration converted to days.
   *
   * @return The duration in days.
   */
  public double getDays() {
    return nanoseconds / (24.0 * 3600.0 * 1_000_000_000.0);
  }
  
  /**
   * Converts a value with unit to nanoseconds.
   *
   * @param value The numeric value.
   * @param unit The unit (e.g., "ns", "ms", "s", etc.).
   * @return The equivalent duration in nanoseconds.
   */
  private long convertToNanoseconds(double value, String unit) {
    switch (unit) {
      case "ns":
        return (long) value;
      case "ms":
        return (long) (value * 1_000_000);
      case "s":
        return (long) (value * 1_000_000_000);
      case "min":
        return (long) (value * 60 * 1_000_000_000);
      case "h":
      case "hr":
        return (long) (value * 3600 * 1_000_000_000);
      case "d":
      case "day":
        return (long) (value * 24 * 3600 * 1_000_000_000);
    }
    return (long) value;
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
