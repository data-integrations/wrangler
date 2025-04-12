/*
 * Copyright © 2017-2022 Cask Data, Inc.
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

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * This class represents a time duration value with a unit (ms, s, m, h, d).
 * It provides methods to parse string representations of time durations and
 * convert between different units.
 */
@PublicEvolving
public class TimeDuration implements Token {
  // Pattern to match formats like "10ms", "1.5s", "2.5m", "1h", "3d"
  private static final Pattern TIME_DURATION_PATTERN = 
      Pattern.compile("^(\\d+(\\.\\d+)?)\\s*(ns|ms|s|m|h|d)$", Pattern.CASE_INSENSITIVE);
  
  private final String original;
  private final double value;
  private final TimeUnit unit;
  
  /**
   * Mapping of string representation to TimeUnit.
   */
  private static TimeUnit getTimeUnitFromString(String unitString) {
    switch (unitString.toLowerCase(Locale.ENGLISH)) {
      case "ns":
        return TimeUnit.NANOSECONDS;
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
        throw new IllegalArgumentException("Unrecognized time unit: " + unitString);
    }
  }
  
  /**
   * Get the standard string representation of a TimeUnit.
   */
  private static String getUnitString(TimeUnit unit) {
    switch (unit) {
      case NANOSECONDS:
        return "ns";
      case MICROSECONDS:
        return "μs";
      case MILLISECONDS:
        return "ms";
      case SECONDS:
        return "s";
      case MINUTES:
        return "m";
      case HOURS:
        return "h";
      case DAYS:
        return "d";
      default:
        throw new IllegalArgumentException("Unrecognized time unit: " + unit);
    }
  }
  
  /**
   * Constructs a TimeDuration from a string representation like "10ms", "1.5s", etc.
   *
   * @param durationString The string representation of the time duration
   * @throws IllegalArgumentException if the string is not a valid time duration
   */
  public TimeDuration(String durationString) {
    this.original = durationString;
    Matcher matcher = TIME_DURATION_PATTERN.matcher(durationString);
    
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid time duration format: " + durationString);
    }
    
    this.value = Double.parseDouble(matcher.group(1));
    this.unit = getTimeUnitFromString(matcher.group(3));
  }
  
  /**
   * Constructs a TimeDuration with specific value and unit.
   *
   * @param value The numeric value
   * @param unit The time unit
   */
  public TimeDuration(double value, TimeUnit unit) {
    this.original = value + getUnitString(unit);
    this.value = value;
    this.unit = unit;
  }
  
  /**
   * Returns the original string representation.
   *
   * @return The original string
   */
  public String getOriginal() {
    return original;
  }
  
  /**
   * Returns the numeric value in the original unit.
   *
   * @return The numeric value
   */
  public double getValue() {
    return value;
  }
  
  /**
   * Returns the unit of this time duration.
   *
   * @return The time unit
   */
  public TimeUnit getUnit() {
    return unit;
  }
  
  /**
   * Returns the total number of nanoseconds represented by this time duration.
   * This is the canonical unit used for internal calculations.
   *
   * @return The total nanoseconds as a long value
   */
  public long getNanoseconds() {
    // Handle fractional parts
    double nanos;
    switch (unit) {
      case NANOSECONDS:
        nanos = value;
        break;
      case MICROSECONDS:
        nanos = value * 1_000;
        break;
      case MILLISECONDS:
        nanos = value * 1_000_000;
        break;
      case SECONDS:
        nanos = value * 1_000_000_000;
        break;
      case MINUTES:
        nanos = value * 60 * 1_000_000_000;
        break;
      case HOURS:
        nanos = value * 60 * 60 * 1_000_000_000;
        break;
      case DAYS:
        nanos = value * 24 * 60 * 60 * 1_000_000_000;
        break;
      default:
        throw new IllegalStateException("Unexpected time unit: " + unit);
    }
    return (long) nanos;
  }
  
  /**
   * Converts this time duration to milliseconds.
   *
   * @return The duration in milliseconds
   */
  public long getMilliseconds() {
    return TimeUnit.NANOSECONDS.toMillis(getNanoseconds());
  }
  
  /**
   * Converts this time duration to seconds.
   *
   * @return The duration in seconds
   */
  public long getSeconds() {
    return TimeUnit.NANOSECONDS.toSeconds(getNanoseconds());
  }
  
  /**
   * Converts this time duration to the specified unit.
   *
   * @param targetUnit The target time unit
   * @return The duration in the target unit (may lose precision for larger units)
   */
  public long convertTo(TimeUnit targetUnit) {
    return targetUnit.convert(getNanoseconds(), TimeUnit.NANOSECONDS);
  }
  
  /**
   * Converts this time duration to a specified unit as a double value (preserving fractional parts).
   *
   * @param targetUnit The target time unit
   * @return The duration in the target unit as a double
   */
  public double convertToDouble(TimeUnit targetUnit) {
    double nanos = getNanoseconds();
    
    switch (targetUnit) {
      case NANOSECONDS:
        return nanos;
      case MICROSECONDS:
        return nanos / 1_000;
      case MILLISECONDS:
        return nanos / 1_000_000;
      case SECONDS:
        return nanos / 1_000_000_000;
      case MINUTES:
        return nanos / (60 * 1_000_000_000.0);
      case HOURS:
        return nanos / (60 * 60 * 1_000_000_000.0);
      case DAYS:
        return nanos / (24 * 60 * 60 * 1_000_000_000.0);
      default:
        throw new IllegalStateException("Unexpected time unit: " + targetUnit);
    }
  }
  
  /**
   * Converts this time duration to a new TimeDuration object in a different unit.
   *
   * @param targetUnit The target unit to convert to
   * @return A new TimeDuration object in the target unit
   */
  public TimeDuration to(TimeUnit targetUnit) {
    if (unit == targetUnit) {
      return this;
    }
    return new TimeDuration(convertToDouble(targetUnit), targetUnit);
  }
  
  /**
   * Formats this time duration with the given precision.
   *
   * @param precision Number of decimal digits to include
   * @return A formatted string representation
   */
  public String format(int precision) {
    String format = "%." + precision + "f%s";
    return String.format(format, value, getUnitString(unit));
  }
  
  /**
   * Creates a TimeDuration object from a number of nanoseconds.
   *
   * @param nanos The number of nanoseconds
   * @return A TimeDuration object representing the specified number of nanoseconds
   */
  public static TimeDuration fromNanos(long nanos) {
    return new TimeDuration(nanos, TimeUnit.NANOSECONDS);
  }
  
  /**
   * Creates a TimeDuration object from a number of milliseconds.
   *
   * @param millis The number of milliseconds
   * @return A TimeDuration object representing the specified number of milliseconds
   */
  public static TimeDuration fromMillis(long millis) {
    return new TimeDuration(millis, TimeUnit.MILLISECONDS);
  }
  
  /**
   * Parses a time duration string, with null handling.
   *
   * @param durationString The string to parse
   * @return A TimeDuration object or null if the input was null
   * @throws IllegalArgumentException if the string is not a valid time duration
   */
  @Nullable
  public static TimeDuration parse(@Nullable String durationString) {
    if (durationString == null || durationString.trim().isEmpty()) {
      return null;
    }
    return new TimeDuration(durationString);
  }
  
  @Override
  public String toString() {
    return original;
  }
  
  @Override
  public TimeDuration value() {
    return this;
  }
  
  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }
  
  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.TIME_DURATION.name());
    object.addProperty("value", original);
    object.addProperty("nanos", getNanoseconds());
    object.addProperty("unit", getUnitString(unit));
    return object;
  }
} 
