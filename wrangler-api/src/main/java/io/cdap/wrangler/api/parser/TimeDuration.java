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
import com.google.gson.JsonPrimitive;

public class TimeDuration implements Token {
  private final long nanoseconds;
  private final String unit;
  private final double value;

  public TimeDuration(String value) {
    String numericPart = value.replaceAll("[^0-9.]", "");
    this.value = Double.parseDouble(numericPart);
    this.unit = value.replaceAll("[0-9.]", "").trim().toLowerCase();

    switch (this.unit) {
      case "ns":
        this.nanoseconds = Math.round(this.value);
        break;
      case "ms":
        this.nanoseconds = Math.round(this.value * 1_000_000);
        break;
      case "s":
        this.nanoseconds = Math.round(this.value * 1_000_000_000);
        break;
      case "min":
        this.nanoseconds = Math.round(this.value * 60 * 1_000_000_000L);
        break;
      case "h":
        this.nanoseconds = Math.round(this.value * 60 * 60 * 1_000_000_000L);
        break;
      default:
        throw new IllegalArgumentException("Unsupported time unit: " + this.unit);
    }
  }

  public long getNanoseconds() {
    return nanoseconds;
  }

  public double getMilliseconds() {
    return nanoseconds / 1_000_000.0;
  }

  public double getSeconds() {
    return nanoseconds / 1_000_000_000.0;
  }

  public double getMinutes() {
    return nanoseconds / (60.0 * 1_000_000_000.0);
  }

  public double getHours() {
    return nanoseconds / (60.0 * 60.0 * 1_000_000_000.0);
  }

  public double getValue() {
    return value;
  }

  public String getUnit() {
    return unit;
  }

  @Override
  public JsonElement toJson() {
    JsonObject json = new JsonObject();
    json.add("value", new JsonPrimitive(value));
    json.add("unit", new JsonPrimitive(unit));
    json.add("nanoseconds", new JsonPrimitive(nanoseconds));
    return json;
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