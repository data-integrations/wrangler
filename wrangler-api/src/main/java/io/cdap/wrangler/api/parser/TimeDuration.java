/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Token implementation for time duration values like "100ms", "2s", "5m", "1d", etc.
 * Internally stores everything in nanoseconds.
 */
public class TimeDuration implements Token {
  private static final Pattern TIME_PATTERN = Pattern.compile("(?i)(\\d+(?:\\.\\d+)?)(ns|us|ms|s|m|h|d)");

  private final double valueInNanoseconds;

  public TimeDuration(String value) {
    this.valueInNanoseconds = parse(value);
  }

  public static double parse(String value) {
    Matcher matcher = TIME_PATTERN.matcher(value.trim());
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid time duration: " + value);
    }

    double number = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(2).toLowerCase();

    switch (unit) {
      case "ns":
        return number;
      case "us":
        return number * 1_000L;
      case "ms":
        return number * 1_000_000L;
      case "s":
        return number * 1_000_000_000L;
      case "m":
        return number * 60L * 1_000_000_000L;
      case "h":
        return number * 60L * 60L * 1_000_000_000L;
      case "d":
        return number * 24L * 60L * 60L * 1_000_000_000L;
      default:
        throw new IllegalArgumentException("Unknown time unit: " + unit);
    }
  }

  public double getNanoseconds() {
    return valueInNanoseconds;
  }

  @Override
  public Object value() {
    return valueInNanoseconds;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(valueInNanoseconds);
  }
}
