/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package io.cdap.wrangler.api.parser;

import com.google.gson.JsonPrimitive;

import java.util.Locale;

/**
 * Token representing a time duration (e.g., 150ms, 2s, 3m, 1h).
 */
public class TimeDuration implements Token {
  private final long milliseconds;
  private final String value;

  public TimeDuration(String value) {
    this.value = value.trim().toLowerCase(Locale.ROOT);
    this.milliseconds = parseTimeDuration(this.value);
  }

  private long parseTimeDuration(String str) {
    if (str.endsWith("ms")) {
      return (long) Double.parseDouble(str.replace("ms", ""));
    } else if (str.endsWith("s")) {
      return (long) (Double.parseDouble(str.replace("s", "")) * 1000);
    } else if (str.endsWith("m")) {
      return (long) (Double.parseDouble(str.replace("m", "")) * 1000 * 60);
    } else if (str.endsWith("h")) {
      return (long) (Double.parseDouble(str.replace("h", "")) * 1000 * 60 * 60);
    } else if (str.endsWith("d")) {
      return (long) (Double.parseDouble(str.replace("d", "")) * 1000L * 60 * 60 * 24);
    } else {
      throw new IllegalArgumentException("Unknown time duration format: " + str);
    }
  }

  public long getMilliseconds() {
    return milliseconds;
  }

  @Override
  public String value() {
    return value;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonPrimitive toJson() {
    return new JsonPrimitive(getMilliseconds());
  }
}
