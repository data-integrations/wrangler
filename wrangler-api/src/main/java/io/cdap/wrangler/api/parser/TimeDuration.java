/*
 * Copyright © 2025 Cask Data, Inc.
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

public class TimeDuration implements Token {
  private final long valueInMillis;
  private final String rawValue;

  public TimeDuration(String value) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Time duration value cannot be null or empty");
    }
    this.rawValue = value.trim();
    this.valueInMillis = parse(this.rawValue);
  }

  private long parse(String value) {
    value = value.trim().toLowerCase();

    if (value.endsWith("ms")) {
      return (long) Double.parseDouble(value.replace("ms", ""));
    } else if (value.endsWith("s")) {
      return (long) (Double.parseDouble(value.replace("s", "")) * 1000);
    } else if (value.endsWith("m")) {
      return (long) (Double.parseDouble(value.replace("m", "")) * 60 * 1000);
    } else if (value.endsWith("h")) {
      return (long) (Double.parseDouble(value.replace("h", "")) * 60 * 60 * 1000);
    }

    throw new IllegalArgumentException("Unsupported time duration unit in value: " + value);
  }

  public long getMillis() {
    return valueInMillis;
  }

  @Override
  public Object value() {
    return valueInMillis;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(valueInMillis);
  }
}
