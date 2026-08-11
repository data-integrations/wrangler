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

import java.util.Locale;

/**
 * Implementation of Token representing a time duration value (e.g., "150ms", "2.1s").
 */
@PublicEvolving
public class TimeDuration implements Token {

  private final String token;   // Original token string
  private final double value;   // Numeric value extracted from token
  private final String unit;    // Unit (e.g., "ms", "s")

  /**
   * Constructs a TimeDuration token by parsing the given string.
   *
   * @param tokenString the raw token string, e.g., "150ms"
   */
  public TimeDuration(String tokenString) {
    this.token = tokenString;
    String trimmed = tokenString.trim();
    int splitIndex = findFirstNonDigitOrDot(trimmed);
    if (splitIndex <= 0 || splitIndex >= trimmed.length()) {
      throw new IllegalArgumentException("Invalid time duration token: " + tokenString);
    }
    String valuePart = trimmed.substring(0, splitIndex);
    // Convert unit to lowercase for consistent matching
    String unitPart = trimmed.substring(splitIndex).toLowerCase(Locale.ENGLISH);

    try {
      this.value = Double.parseDouble(valuePart);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid numeric value in token: " + tokenString, e);
    }
    this.unit = unitPart;
    if (!unit.equals("ms") && !unit.equals("s")) {
        throw new IllegalArgumentException("Unknown time unit: " + unit);
    }
  }

  /**
   * Returns the canonical value in milliseconds.
   *
   * @return value in milliseconds
   */
  public long getMilliseconds() {
    switch (unit) {
      case "ms":
        return (long) value;
      case "s":
        return (long) (value * 1000);
      default:
        throw new IllegalArgumentException("Unknown time unit: " + unit);
    }
  }

  // Helper method: finds the first character that is not a digit or a dot.
  private int findFirstNonDigitOrDot(String str) {
    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);
      if (!(Character.isDigit(c) || c == '.')) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public Object value() {
    // Return the canonical representation, e.g., the value in milliseconds.
    return getMilliseconds();
  }

  @Override
  public TokenType type() {
    // Ensure that TokenType has a TIME_DURATION entry.
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    JsonObject json = new JsonObject();
    json.addProperty("token", token);
    json.addProperty("value", value);
    json.addProperty("unit", unit);
    json.addProperty("milliseconds", getMilliseconds());
    return json;
  }
}
