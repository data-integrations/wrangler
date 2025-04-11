/*
 * Copyright © 2017-2025 Cask Data, Inc.
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
import com.google.gson.JsonPrimitive;
/**
 * Token implementation for time duration values like "500ms", "2s", "3h", etc.
 */

public class TimeDuration implements Token {
  private final String original;
  private final long millis;

  public TimeDuration(String value) {
    this.original = value.trim().toLowerCase();

    if (original.endsWith("ms")) {
      millis = parseValue(original, "ms", 1);
    } else if (original.endsWith("s")) {
      millis = parseValue(original, "s", 1000L);
    } else if (original.endsWith("m")) {
      millis = parseValue(original, "m", 60_000L);
    } else if (original.endsWith("h")) {
      millis = parseValue(original, "h", 3_600_000L);
    } else {
      throw new IllegalArgumentException("Unsupported time unit in: " + value);
    }
  }

  private long parseValue(String input, String suffix, long multiplier) {
    double number = Double.parseDouble(input.substring(0, input.length() - suffix.length()));
    return (long) (number * multiplier);
  }

  public long getMillis() {
    return millis;
  }

  @Override
  public Object value() {
    return millis;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(millis);
  }

  @Override
  public String toString() {
    return original;
  }
}
