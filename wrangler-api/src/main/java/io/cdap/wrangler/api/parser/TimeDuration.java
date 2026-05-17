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
 * This class {@code TimeDuration} represents a time duration specification token.
 * It supports formats like "1s", "1m", "1h", or "1d".
 */
@PublicEvolving
public class TimeDuration implements Token, Serializable {
  private static final long serialVersionUID = 1L;
  private final String value;
  private static final Pattern TIME_DURATION_PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)(s|m|h|d)");

  public TimeDuration(String value) {
    this.value = value;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public Object value() {
    return value;
  }

  @Override
  public String toString() {
    return value;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", type().name());
    object.addProperty("value", value);
    return object;
  }

  public long getMilliseconds() {
    Matcher matcher = TIME_DURATION_PATTERN.matcher(value.toLowerCase());
    if (!matcher.matches()) {
      return 0L;
    }
    double duration = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(2);
    switch (unit.toLowerCase()) {
      case "s":
      case "seconds":
        return Math.round(duration * 1000);
      case "m":
      case "minutes":
        return Math.round(duration * 60 * 1000);
      case "h":
      case "hours":
        return Math.round(duration * 60 * 60 * 1000);
      case "d":
      case "days":
        return Math.round(duration * 24 * 60 * 60 * 1000);
      default:
        return 0L;
    }
  }

  public String convertTo(String targetUnit) {
    long milliseconds = getMilliseconds();
    if (milliseconds == 0) {
      return "0" + targetUnit; // Return zero with target unit
    }

    double convertedValue;
    switch (targetUnit.toLowerCase()) {
      case "s":
      case "seconds":
        convertedValue = milliseconds / 1000.0;
        targetUnit = "s";
        break;
      case "m":
      case "minutes":
        convertedValue = milliseconds / (60.0 * 1000);
        targetUnit = "m";
        break;
      case "h":
      case "hours":
        convertedValue = milliseconds / (60.0 * 60 * 1000);
        targetUnit = "h";
        break;
      case "d":
      case "days":
        convertedValue = milliseconds / (24.0 * 60 * 60 * 1000);
        targetUnit = "d";
        break;
      default:
        return value; // Return original value if invalid unit
    }

    // Format with at most one decimal place, removing trailing zeros
    String formatted = String.format("%.1f", convertedValue).replaceAll("\\.?0*$", "");
    return formatted + targetUnit;
  }
} 
