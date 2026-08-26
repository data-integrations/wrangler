/*
 * Copyright © 2017-2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import java.util.Locale;

/**
 * Represents a time duration parsed from a string (e.g., "150ms").
 */
public class TimeDuration implements Token {
  private final long millis;

  /**
   * Constructs a {@code TimeDuration} by parsing the input string.
   *
   * @param value the string representation (e.g., "150ms", "2s")
   * @throws IllegalArgumentException if the value is invalid
   */
  public TimeDuration(String value) {
    this.millis = parseTimeDuration(value);
  }

  /**
   * Parses the input string to calculate the duration in milliseconds.
   *
   * @param value the string to parse
   * @return the duration in milliseconds
   * @throws IllegalArgumentException if the format or number is invalid
   */
  private long parseTimeDuration(String value) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Time duration value cannot be null or empty");
    }
    String val = value.toLowerCase(Locale.ROOT).trim();
    try {
      if (val.endsWith("ms")) {
        return Long.parseLong(val.replace("ms", "").trim());
      } else if (val.endsWith("s")) {
        return Long.parseLong(val.replace("s", "").trim()) * 1000L;
      } else if (val.endsWith("m")) {
        return Long.parseLong(val.replace("m", "").trim()) * 60L * 1000L;
      } else if (val.endsWith("h")) {
        return Long.parseLong(val.replace("h", "").trim()) * 3600L * 1000L;
      } else if (val.endsWith("d")) {
        return Long.parseLong(val.replace("d", "").trim()) * 24L * 3600L * 1000L;
      } else {
        throw new IllegalArgumentException("Invalid time duration format: " + value);
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid number in time duration: " + value, e);
    }
  }

  /**
   * Returns the time duration in milliseconds.
   *
   * @return the number of milliseconds
   */
  public long getMillis() {
    return millis;
  }

  /**
   * Returns the value of the time duration.
   *
   * @return the duration as a {@code Long}
   */
  @Override
  public Object value() {
    return millis;
  }

  /**
   * Returns the token type for this time duration.
   *
   * @return the {@code TokenType.TIME_DURATION}
   */
  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  /**
   * Converts the time duration to a JSON representation.
   *
   * @return a JSON element containing the duration
   */
  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(millis);
  }
}
