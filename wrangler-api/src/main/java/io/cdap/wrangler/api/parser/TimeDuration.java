/*
 * Copyright © 2023-2025 Cask Data, Inc.
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

import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.TimeUnit;

/**
 * The TimeDuration class wraps a time duration value and unit in an object.
 * An object of type {@code TimeDuration} contains the value in nanoseconds as a long
 * as well as the original string representation.
 * 
 * <p>In addition, this class provides methods to extract the
 * value held by this wrapper object in various units (nanoseconds, microseconds, milliseconds, 
 * seconds, minutes, hours), and the method for extracting the type of the token.</p>
 * 
 * <p>This class supports parsing durations in the format of a number followed by a unit,
 * such as "10ms", "1.5s", "30min", "2h". Both full unit names and abbreviations are supported.</p>
 *
 * @see Bool
 * @see BoolList
 * @see ColumnName
 * @see ColumnNameList
 * @see DirectiveName
 * @see Numeric
 * @see NumericList
 * @see Properties
 * @see Ranges
 * @see Expression
 * @see Text
 * @see TextList
 * @see ByteSize
 */
@PublicEvolving
public class TimeDuration implements Token {
  private static final Pattern TIME_PATTERN = Pattern.compile("^([\\d.]+)\\s*(ms|s|min|h)$");

  // Constants for unit conversion (to nanoseconds)
  private static final long MILLISECOND = 1_000_000L;
  private static final long SECOND = 1_000 * MILLISECOND;
  private static final long MINUTE = 60 * SECOND;
  private static final long HOUR = 60 * MINUTE;

  private final String original;
  private final long nanoseconds;
  
  /**
   * Constructs a TimeDuration from a string representation like "10ms", "1.5s", etc.
   * 
   * @param value String representation of a time duration with unit
   * @throws ParseException If the input string cannot be parsed as a valid time duration
   */
  public TimeDuration(String value) throws ParseException {
    this.original = value;
    this.nanoseconds = parseNanoseconds(value);
  }
  
  /**
   * Parses a string representation of time duration into a value in nanoseconds.
   * 
   * @param input String containing a number followed by a unit (ms, s, min, h)
   * @return The value in nanoseconds
   * @throws ParseException If the input string cannot be parsed
   */
  private long parseNanoseconds(String input) throws ParseException {
    if (input == null || input.trim().isEmpty()) {
      throw new ParseException("Empty input for time duration", 0);
    }

    Matcher matcher = TIME_PATTERN.matcher(input.trim());
    if (!matcher.matches()) {
      throw new ParseException("Invalid time duration format: " + input, 0);
    }

    double value = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(2);

    if (value < 0) {
      throw new ParseException("Time duration cannot be negative: " + input, 0);
    }

    switch (unit) {
      case "ms":
        return Math.round(value * MILLISECOND);
      case "s":
        return Math.round(value * SECOND);
      case "min":
        return Math.round(value * MINUTE);
      case "h":
        return Math.round(value * HOUR);
      default:
        throw new ParseException("Unknown time unit: " + unit, matcher.start(2));
    }
  }
  
  /**
   * Returns the original string representation of this {@code TimeDuration} object.
   *
   * @return The string representation of this object.
   */
  public String getOriginalString() {
    return original;
  }

  /**
   * Returns the value of this {@code TimeDuration} object in nanoseconds.
   *
   * @return The value in nanoseconds.
   */
  public long getNanoseconds() {
    return nanoseconds;
  }
  
  /**
   * Returns the value of this {@code TimeDuration} object in microseconds.
   *
   * @return The value in microseconds.
   */
  public double getMicroseconds() {
    return nanoseconds / 1000.0;
  }
  
  /**
   * Returns the value of this {@code TimeDuration} object in milliseconds.
   *
   * @return The value in milliseconds.
   */
  public double getMilliseconds() {
    return nanoseconds / (double) MILLISECOND;
  }
  
  /**
   * Returns the value of this {@code TimeDuration} object in seconds.
   *
   * @return The value in seconds.
   */
  public double getSeconds() {
    return nanoseconds / (double) SECOND;
  }
  
  /**
   * Returns the value of this {@code TimeDuration} object in minutes.
   *
   * @return The value in minutes.
   */
  public double getMinutes() {
    return nanoseconds / (double) MINUTE;
  }
  
  /**
   * Returns the value of this {@code TimeDuration} object in hours.
   *
   * @return The value in hours.
   */
  public double getHours() {
    return nanoseconds / (double) HOUR;
  }

  @Override
  public String value() {
    return original;
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
    object.addProperty("nanoseconds", nanoseconds);
    return object;
  }
  
  @Override
  public String toString() {
    return original;
  }
}