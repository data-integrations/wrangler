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

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * The TimeDuration class wraps time duration values with units (ms, s, m, h)
 * and provides methods to convert between different units.
 */
@PublicEvolving
public class TimeDuration implements Token {
  private final long milliseconds;
  private final String originalValue;

  public TimeDuration(String value) {
    this.originalValue = value;
    this.milliseconds = parseMilliseconds(value);
  }

  private long parseMilliseconds(String value) {
    value = value.trim().toLowerCase();
    long multiplier = 1;
    String number = value;

    if (value.endsWith("ms")) {
      multiplier = 1;
      number = value.substring(0, value.length() - 2);
    } else if (value.endsWith("s")) {
      multiplier = 1000;
      number = value.substring(0, value.length() - 1);
    } else if (value.endsWith("m")) {
      multiplier = 60 * 1000;
      number = value.substring(0, value.length() - 1);
    } else if (value.endsWith("h")) {
      multiplier = 60 * 60 * 1000;
      number = value.substring(0, value.length() - 1);
    }

    try {
      double val = Double.parseDouble(number);
      return (long) (val * multiplier);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid time duration format: " + value);
    }
  }

  @Override
  public Long value() {
    return milliseconds;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.TIME_DURATION.name());
    object.addProperty("value", originalValue);
    object.addProperty("milliseconds", milliseconds);
    return object;
  }

  public long getMilliseconds() {
    return milliseconds;
  }

  public double getSeconds() {
    return milliseconds / 1000.0;
  }

  public double getMinutes() {
    return milliseconds / (60.0 * 1000.0);
  }

  public double getHours() {
    return milliseconds / (60.0 * 60.0 * 1000.0);
  }
} 