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

 
// --- File: TimeDuration.java ---
package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

/**
 * Token representing a time duration value (e.g., "150ms", "2.5s").
 */
public class TimeDuration implements Token {
  private final long milliseconds;

  public TimeDuration(String value) {
    String unit = value.replaceAll("[0-9.]", "").toLowerCase();
    double number = Double.parseDouble(value.replaceAll("[^0-9.]", ""));
    switch (unit) {
      case "s": this.milliseconds = (long) (number * 1000); break;
      case "ms": this.milliseconds = (long) (number); break;
      case "min": this.milliseconds = (long) (number * 60000); break;
      default: this.milliseconds = (long) number; break;
    }
  }

  public long getMilliseconds() {
    return milliseconds;
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


