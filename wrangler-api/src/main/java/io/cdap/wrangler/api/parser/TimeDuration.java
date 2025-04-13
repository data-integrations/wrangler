/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class representing time duration values with units (ns, us, ms, s, m, h, d).
 */
@PublicEvolving
public class TimeDuration implements Token {
  private static final Pattern TIME_DURATION_PATTERN = 
      Pattern.compile("([0-9]+(?:\\.[0-9]+)?)\\s*([nmu]s|[smhd])", Pattern.CASE_INSENSITIVE);
  private static final long NANOS_IN_MICRO = 1000L;
  private static final long NANOS_IN_MILLI = NANOS_IN_MICRO * 1000L;
  private static final long NANOS_IN_SECOND = NANOS_IN_MILLI * 1000L;
  private static final long NANOS_IN_MINUTE = NANOS_IN_SECOND * 60L;
  private static final long NANOS_IN_HOUR = NANOS_IN_MINUTE * 60L;
  private static final long NANOS_IN_DAY = NANOS_IN_HOUR * 24L;

  private final String rawValue;
  private final double numericValue;
  private final String unit;
  private final long nanoseconds;

  /**
   * Constructor for TimeDuration.
   *
   * @param value String representation of time duration (e.g., "100ms", "2.5s")
   */
  public TimeDuration(String value) {
    this.rawValue = value;
    
    Matcher matcher = TIME_DURATION_PATTERN.matcher(value.trim());
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid time duration format: " + value);
    }
    
    this.numericValue = Double.parseDouble(matcher.group(1));
    this.unit = matcher.group(2).toLowerCase();
    this.nanoseconds = calculateNanoseconds(numericValue, unit);
  }

  /**
   * Calculate nanoseconds based on the numeric value and unit.
   *
   * @param value Numeric value
   * @param unit Unit (ns, us, ms, s, m, h, d)
   * @return Number of nanoseconds
   */
  private long calculateNanoseconds(double value, String unit) {
    switch (unit.toLowerCase()) {
      case "ns":
        return (long) value;
      case "us":
        return (long) (value * NANOS_IN_MICRO);
      case "ms":
        return (long) (value * NANOS_IN_MILLI);
      case "s":
        return (long) (value * NANOS_IN_SECOND);
      case "m":
        return (long) (value * NANOS_IN_MINUTE);
      case "h":
        return (long) (value * NANOS_IN_HOUR);
      case "d":
        return (long) (value * NANOS_IN_DAY);
      default:
        throw new IllegalArgumentException("Unsupported time duration unit: " + unit);
    }
  }

  /**
   * Gets the raw string value.
   *
   * @return Raw string value
   */
  @Override
  public String value() {
    return rawValue;
  }

  /**
   * Gets the numeric part of the time duration.
   *
   * @return Numeric value
   */
  public double getNumericValue() {
    return numericValue;
  }

  /**
   * Gets the unit part of the time duration.
   *
   * @return Unit string
   */
  public String getUnit() {
    return unit;
  }

  /**
   * Gets the value converted to nanoseconds.
   *
   * @return Number of nanoseconds
   */
  public long getNanoseconds() {
    return nanoseconds;
  }

  /**
   * Gets the value converted to microseconds.
   *
   * @return Number of microseconds
   */
  public double getMicroseconds() {
    return nanoseconds / (double) NANOS_IN_MICRO;
  }

  /**
   * Gets the value converted to milliseconds.
   *
   * @return Number of milliseconds
   */
  public double getMilliseconds() {
    return nanoseconds / (double) NANOS_IN_MILLI;
  }

  /**
   * Gets the value converted to seconds.
   *
   * @return Number of seconds
   */
  public double getSeconds() {
    return nanoseconds / (double) NANOS_IN_SECOND;
  }

  /**
   * Gets the value converted to minutes.
   *
   * @return Number of minutes
   */
  public double getMinutes() {
    return nanoseconds / (double) NANOS_IN_MINUTE;
  }

  /**
   * Gets the value converted to hours.
   *
   * @return Number of hours
   */
  public double getHours() {
    return nanoseconds / (double) NANOS_IN_HOUR;
  }

  /**
   * Gets the value converted to days.
   *
   * @return Number of days
   */
  public double getDays() {
    return nanoseconds / (double) NANOS_IN_DAY;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.TIME_DURATION.name());
    object.addProperty("value", rawValue);
    object.addProperty("nanoseconds", nanoseconds);
    return object;
  }

  @Override
  public String toString() {
    return rawValue;
  }
}