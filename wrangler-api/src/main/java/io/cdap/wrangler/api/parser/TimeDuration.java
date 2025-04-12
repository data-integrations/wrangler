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

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A {@link Token} type for representing time duration values with units (e.g., 10ms, 1.5s).
 */
@PublicEvolving
public class TimeDuration implements Token {
  private static final Pattern TIME_DURATION_PATTERN = 
      Pattern.compile("^(\\d+(?:\\.\\d+)?)\\s*([num]?s|m|h|d)$", Pattern.CASE_INSENSITIVE);
  
  private final String original;
  private final long nanoseconds;

  public TimeDuration(String value) {
    this.original = value;
    Matcher matcher = TIME_DURATION_PATTERN.matcher(value.trim());
    if (!matcher.matches()) {
      throw new IllegalArgumentException(String.format(
          "Invalid time duration format '%s'. Expected format is <number><unit> where unit is ms, s, m, h, or d", 
          value));
    }

    double duration = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(2).toLowerCase();

    switch (unit) {
      case "ns":
        nanoseconds = (long) duration;
        break;
      case "us":
        nanoseconds = (long) (duration * 1000);
        break;
      case "ms":
        nanoseconds = (long) (duration * 1000_000);
        break;
      case "s":
        nanoseconds = (long) (duration * 1000_000_000);
        break;
      case "m":
        nanoseconds = (long) (duration * 60 * 1000_000_000L);
        break;
      case "h":
        nanoseconds = (long) (duration * 60 * 60 * 1000_000_000L);
        break;
      case "d":
        nanoseconds = (long) (duration * 24 * 60 * 60 * 1000_000_000L);
        break;
      default:
        throw new IllegalArgumentException("Unsupported time duration unit: " + unit);
    }
  }

  @Override
  public Object value() {
    return nanoseconds;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", type().name());
    object.addProperty("value", original);
    object.addProperty("nanoseconds", nanoseconds);
    return object;
  }

  /**
   * @return the duration in nanoseconds
   */
  public long getNanos() {
    return nanoseconds;
  }

  /**
   * @return the original string representation
   */
  public String getOriginal() {
    return original;
  }
}
