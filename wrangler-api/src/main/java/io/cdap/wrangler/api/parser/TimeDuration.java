/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

/**
 * Represents a time duration value with methods to parse and retrieve the duration in milliseconds
 * and optionally in nanoseconds.
 */
public class TimeDuration implements Token {
  private final String original;
  private final long milliseconds;

  /**
   * Constructs a TimeDuration object from the provided value.
   *
   * @param value the time duration value as a string (e.g., "10s", "2m")
   */
  public TimeDuration(String value) {
    this.original = value;
    this.milliseconds = parse(value);
  }

  /**
   * Parses the provided time duration string and converts it into milliseconds.
   *
   * @param value the time duration string (e.g., "10s", "2m")
   * @return the duration in milliseconds
   * @throws IllegalArgumentException if the time duration is invalid
   */
  private long parse(String value) {
    value = value.trim().toLowerCase();
    double number = Double.parseDouble(value.replaceAll("[^0-9.]", ""));
    
    if (value.endsWith("ms")) {
      return (long) number;
    } 
    if (value.endsWith("s")) {
      return (long) (number * 1000);
    } 
    if (value.endsWith("m")) {
      return (long) (number * 60 * 1000);
    }
    if (value.endsWith("h")) {
      return (long) (number * 60 * 60 * 1000);
    }
    
    throw new IllegalArgumentException("Invalid time duration: " + value);
  }

  /**
   * Returns the time duration in milliseconds.
   *
   * @return the duration in milliseconds
   */
  public long getMilliseconds() {
    return milliseconds;
  }

  /**
   * Returns the time duration in nanoseconds by converting milliseconds to nanoseconds.
   *
   * @return the duration in nanoseconds
   */
  public long getTimeInNano() {
    return milliseconds * 1_000_000L;
  }

  @Override
  public Object value() {
    return milliseconds;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(milliseconds);
  }
}
