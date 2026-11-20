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
import com.google.gson.JsonPrimitive;
import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * Token implementation for parsing time durations like "100ms", "2s", "1.5m", "3h".
 */
@PublicEvolving
public class TimeDuration implements Token {
  private final long milliseconds;
  private final String original;

  public TimeDuration(String value) {
    this.original = value;
    String lower = value.trim().toLowerCase();
    double numericValue;

    if (lower.endsWith("ms")) {
      numericValue = Double.parseDouble(lower.replace("ms", ""));
      this.milliseconds = (long) numericValue;
    } else if (lower.endsWith("s")) {
      numericValue = Double.parseDouble(lower.replace("s", ""));
      this.milliseconds = (long) (numericValue * 1000);
    } else if (lower.endsWith("m")) {
      numericValue = Double.parseDouble(lower.replace("m", ""));
      this.milliseconds = (long) (numericValue * 60 * 1000);
    } else if (lower.endsWith("h")) {
      numericValue = Double.parseDouble(lower.replace("h", ""));
      this.milliseconds = (long) (numericValue * 60 * 60 * 1000);
    } else {
      throw new IllegalArgumentException(
        "Unsupported time unit. Supported units: ms, s, m, h. Input: " + value
      );
    }
  }

  public long getMilliseconds() {
    return milliseconds;
  }

  @Override
  public Object value() {
    return original;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(original);
  }

  @Override
  public String toString() {
    return milliseconds + " ms";
  }
}
