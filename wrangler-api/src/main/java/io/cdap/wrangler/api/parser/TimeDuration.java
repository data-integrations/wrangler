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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * A {@link Token} for representing time durations.
 */
@PublicEvolving
public class TimeDuration implements Token {
  private static final Pattern TIME_DURATION_PATTERN = Pattern.compile("(\\d+)([nsuµm]s|[smhd])");
  private final long nanoseconds;

  public TimeDuration(String value) {
    Matcher matcher = TIME_DURATION_PATTERN.matcher(value);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid time duration format: " + value);
    }

    long duration = Long.parseLong(matcher.group(1));
    String unit = matcher.group(2).toLowerCase();

    switch (unit) {
      case "ns":
        nanoseconds = duration;
        break;
      case "µs":
      case "us":
        nanoseconds = duration * 1000;
        break;
      case "ms":
        nanoseconds = duration * 1000_000;
        break;
      case "s":
        nanoseconds = duration * 1000_000_000;
        break;
      case "m":
        nanoseconds = duration * 60L * 1000_000_000;
        break;
      case "h":
        nanoseconds = duration * 3600L * 1000_000_000;
        break;
      case "d":
        nanoseconds = duration * 86400L * 1000_000_000;
        break;
      default:
        throw new IllegalArgumentException("Invalid time duration unit: " + unit);
    }
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", "time_duration");
    object.addProperty("value", nanoseconds);
    return object;
  }

  @Override
  public Object value() {
    return nanoseconds;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }
} 