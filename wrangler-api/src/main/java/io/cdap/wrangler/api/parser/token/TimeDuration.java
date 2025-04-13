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

package io.cdap.wrangler.api.parser.token;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TimeDuration class represents a time duration value with a time unit (ns, ms, s, m, h, d).
 * It parses a string representation of a time duration and provides methods to retrieve
 * the value in different time units.
 * <p>
 * The class handles time durations in various formats such as "100ms", "5s", "2.5h", etc.
 * It also supports case-insensitive unit specifications and optional whitespace between
 * the numeric value and the unit.
 */
public class TimeDuration implements Token {
  // Regular expression pattern to match time duration strings
  // Supports optional whitespace between value and unit
  // Set of valid time units (lowercase)
  private static final Set<String> VALID_UNITS = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList("ns", "ms", "s", "m", "h", "d")));
  // Set of valid time units (lowercase)
  private static final Pattern PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)\\s*([a-zA-Z]+)");
  
  // Map of unit symbols to time unit names for better error messages
  private static final Map<String, String> UNIT_NAMES;
  
  static {
    Map<String, String> unitMap = new HashMap<>();
    unitMap.put("ns", "nanoseconds");
    unitMap.put("ms", "milliseconds");
    unitMap.put("s", "seconds");
    unitMap.put("m", "minutes");
    unitMap.put("h", "hours");
    unitMap.put("d", "days");
    UNIT_NAMES = Collections.unmodifiableMap(unitMap);
  }
  
  private final String value;
  private final double numValue;
  private final String unit;
  private final long nanos;

  /**
   * Constructor to create a TimeDuration from a string.
   *
   * @param value String representation of the time duration, e.g. "100ms", "5s", "2.5h"
   * @throws IllegalArgumentException If the input string doesn't match the expected format,
   *                                  contains an invalid unit, or specifies a negative duration
   */
  public TimeDuration(String value) {
    this.value = value;
    Matcher matcher = PATTERN.matcher(value);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
        String.format("Invalid time duration format: '%s'. Expected format: <number><unit>, e.g. 10ms", value));
    }
    
    try {
      this.numValue = Double.parseDouble(matcher.group(1));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
        String.format("Invalid numeric value in time duration: '%s'", matcher.group(1)), e);
    }
    
    // Validate that the value is positive
    if (this.numValue < 0) {
      throw new IllegalArgumentException("Time duration cannot be negative: " + value);
    }
    
    this.unit = matcher.group(2).toLowerCase(Locale.ENGLISH);
    
    // Validate the unit
    if (!VALID_UNITS.contains(this.unit)) {
      throw new IllegalArgumentException(
        String.format("Invalid time unit: '%s'. Expected one of: ns, ms, s, m, h, d", this.unit));
    }
    
    this.nanos = calculateNanos(this.numValue, this.unit);
  }

  /**
   * Calculates the equivalent nanoseconds from a value and unit.
   *
   * @param value Numeric value
   * @param unit Unit string (ns, ms, s, m, h, d)
   * @return Number of nanoseconds
   * @throws IllegalArgumentException If the unit is not recognized
   */
  private long calculateNanos(double value, String unit) {
    switch (unit) {
      case "ns":
        return (long) value;
      case "ms":
        return (long) (value * TimeUnit.MILLISECONDS.toNanos(1));
      case "s":
        return (long) (value * TimeUnit.SECONDS.toNanos(1));
      case "m":
        return (long) (value * TimeUnit.MINUTES.toNanos(1));
      case "h":
        return (long) (value * TimeUnit.HOURS.toNanos(1));
      case "d":
        return (long) (value * TimeUnit.DAYS.toNanos(1));
      default:
        throw new IllegalArgumentException("Unknown time unit: " + unit);
    }
  }

  /**
   * Returns the original string value of this token.
   *
   * @return the original string value
   */
  @Override
  public Object value() {
    return value;
  }

  /**
   * Returns the token type - TIME_DURATION.
   *
   * @return TokenType.TIME_DURATION
   */
  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  /**
   * Converts this object to a JSON representation.
   *
   * @return JsonElement representing this object
   */
  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", type().name());
    object.addProperty("value", value);
    object.addProperty("nanos", nanos);
    object.addProperty("unit", unit);
    object.addProperty("numericValue", numValue);
    return object;
  }

  /**
   * Returns the duration in nanoseconds.
   *
   * @return The duration in nanoseconds
   */
  public long getNanos() {
    return nanos;
  }

  /**
   * Returns the duration in milliseconds.
   *
   * @return The duration in milliseconds
   */
  public double getMillis() {
    // Convert to double for precise floating-point representation
    return nanos / (double) TimeUnit.MILLISECONDS.toNanos(1);
  }

  /**
   * Returns the duration in seconds.
   *
   * @return The duration in seconds
   */
  public double getSeconds() {
    // Convert to double for precise floating-point representation
    return nanos / (double) TimeUnit.SECONDS.toNanos(1);
  }

  /**
   * Returns the duration in minutes.
   *
   * @return The duration in minutes
   */
  public double getMinutes() {
    // Convert to double for precise floating-point representation
    return nanos / (double) TimeUnit.MINUTES.toNanos(1);
  }

  /**
   * Returns the duration in hours.
   *
   * @return The duration in hours
   */
  public double getHours() {
    // Convert to double for precise floating-point representation
    return nanos / (double) TimeUnit.HOURS.toNanos(1);
  }

  /**
   * Returns the duration in days.
   *
   * @return The duration in days
   */
  public double getDays() {
    // Convert to double for precise floating-point representation
    return nanos / (double) TimeUnit.DAYS.toNanos(1);
  }

  /**
   * Returns the numeric value parsed from the original string.
   *
   * @return The numeric value
   */
  public double getNumericValue() {
    return numValue;
  }

  /**
   * Returns the unit parsed from the original string.
   *
   * @return The unit string (ns, ms, s, m, h, d)
   */
  public String getUnit() {
    return unit;
  }
  
  /**
   * Returns the human-readable name of the unit (e.g., "seconds" for "s").
   *
   * @return The human-readable unit name
   */
  public String getUnitName() {
    return UNIT_NAMES.getOrDefault(unit, unit);
  }
  
  /**
   * Creates a new TimeDuration with a different unit but equivalent duration.
   *
   * @param targetUnit The unit to convert to ("ns", "ms", "s", "m", "h", "d")
   * @return A new TimeDuration with the specified unit
   * @throws IllegalArgumentException If the target unit is not recognized
   */
  public TimeDuration convertToUnit(String targetUnit) {
    String normalizedUnit = targetUnit.toLowerCase(Locale.ENGLISH);
    if (!VALID_UNITS.contains(normalizedUnit)) {
      throw new IllegalArgumentException(
        String.format("Invalid target unit: '%s'. Expected one of: ns, ms, s, m, h, d", targetUnit));
    }
    
    double convertedValue;
    switch (normalizedUnit) {
      case "ns":
        convertedValue = nanos;
        break;
      case "ms":
        convertedValue = getMillis();
        break;
      case "s":
        convertedValue = getSeconds();
        break;
      case "m":
        convertedValue = getMinutes();
        break;
      case "h":
        convertedValue = getHours();
        break;
      case "d":
        convertedValue = getDays();
        break;
      default:
        throw new IllegalArgumentException("Unknown time unit: " + normalizedUnit);
    }
    
    return new TimeDuration(convertedValue + normalizedUnit);
  }
  
  /**
   * Checks if this duration is equivalent to another duration.
   *
   * @param other The other TimeDuration to compare with
   * @return true if both durations represent the same amount of time, false otherwise
   */
  public boolean isEquivalentTo(TimeDuration other) {
    return other != null && this.nanos == other.nanos;
  }

  @Override
  public String toString() {
    return value;
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
    return nanos == that.nanos;
  }
  
  @Override
  public int hashCode() {
    return Long.hashCode(nanos);
  }
}
