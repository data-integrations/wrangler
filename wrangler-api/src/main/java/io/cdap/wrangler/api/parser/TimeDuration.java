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

public class TimeDuration implements Token {
  private final String rawValue;

  public TimeDuration(String value) {
    this.rawValue = value.trim().toLowerCase();
  }

  public long getMilliseconds() {
    if (rawValue.endsWith("ms")) {
      return Long.parseLong(rawValue.replace("ms", ""));
    } else if (rawValue.endsWith("s")) {
      return (long)(Double.parseDouble(rawValue.replace("s", "")) * 1000);
    } else if (rawValue.endsWith("min")) {
      return (long)(Double.parseDouble(rawValue.replace("min", "")) * 60 * 1000);
    } else if (rawValue.endsWith("h")) {
      return (long)(Double.parseDouble(rawValue.replace("h", "")) * 3600 * 1000);
    } else {
      throw new IllegalArgumentException("Unknown time unit in: " + rawValue);
    }
  }

  @Override
  public Object value() {
    return rawValue;
  }

  @Override
  public TokenType type() {
    return TokenType.TEXT; // Or add TIME_DURATION to TokenType enum if allowed
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(rawValue);
  }

  @Override
  public String toString() {
    return rawValue;
  }
}
