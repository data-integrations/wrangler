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
 * Implementation of Token representing a byte size value (e.g., "10KB", "1.5MB").
 */
@PublicEvolving
public class ByteSize implements Token {

  private final String token;   // Original token string
  private final double value;   // Numeric value extracted from token
  private final String unit;    // Unit (e.g., "KB", "MB")

  /**
   * Constructs a ByteSize token by parsing the given string.
   *
   * @param tokenString the raw token string, e.g., "10KB"
   */
  public ByteSize(String tokenString) {
    this.token = tokenString;
    String trimmed = tokenString.trim();
    int splitIndex = findFirstNonDigitOrDot(trimmed);
    if (splitIndex <= 0 || splitIndex >= trimmed.length()) {
      throw new IllegalArgumentException("Invalid byte size token: " + tokenString);
    }
    String valuePart = trimmed.substring(0, splitIndex);
    String unitPart = trimmed.substring(splitIndex).trim().toUpperCase(Locale.ENGLISH);

    try {
      this.value = Double.parseDouble(valuePart);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid numeric value in token: " + tokenString, e);
    }
    this.unit = unitPart;
    if (!unit.equals("B") && !unit.equals("KB") && !unit.equals("MB") && !unit.equals("GB") && !unit.equals("TB")) {
        throw new IllegalArgumentException("Unknown byte unit: " + unit);
    }
  }

  /**
   * Returns the canonical value in bytes.
   *
   * @return value in bytes
   */
  public long getBytes() {
    switch (unit) {
      case "B":
        return (long) value;
      case "KB":
        return (long) (value * 1024);
      case "MB":
        return (long) (value * 1024 * 1024);
      case "GB":
        return (long) (value * 1024 * 1024 * 1024);
      case "TB":
        return (long) (value * 1024L * 1024L * 1024L * 1024L);
      default:
        throw new IllegalArgumentException("Unknown byte unit: " + unit);
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
    // Return the canonical representation, i.e., the value in bytes.
    return getBytes();
  }

  @Override
  public TokenType type() {
    // Ensure that TokenType has a BYTE_SIZE entry.
    return TokenType.BYTE_SIZE;
  }

  @Override
  public JsonElement toJson() {
    JsonObject json = new JsonObject();
    json.addProperty("token", token);
    json.addProperty("value", value);
    json.addProperty("unit", unit);
    json.addProperty("bytes", getBytes());
    return json;
  }
}
