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
 * A token that represents a byte size value with a unit suffix (e.g., "10KB", "5.5GB").
 * The value is stored in its canonical form (total bytes).
 */
@PublicEvolving
public class ByteSize implements Token, Serializable {
  private static final long serialVersionUID = 1L;
  private static final Pattern BYTE_SIZE_PATTERN = Pattern.compile(
    "^([0-9]+(?:\\.[0-9]+)?)([KMGTP]?B)$", 
    Pattern.CASE_INSENSITIVE
  );
  
  private final long bytes;
  private final String originalValue;

  /**
   * Creates a new ByteSize token from a string representation.
   *
   * @param value The string representation of the byte size (e.g., "10KB", "5.5GB")
   * @throws IllegalArgumentException if the value cannot be parsed
   */
  public ByteSize(String value) {
    this.originalValue = value;
    Matcher matcher = BYTE_SIZE_PATTERN.matcher(value);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid byte size format: " + value);
    }

    double number = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(2).toUpperCase();

    switch (unit) {
      case "B":
        bytes = (long) number;
        break;
      case "KB":
        bytes = (long) (number * 1024);
        break;
      case "MB":
        bytes = (long) (number * 1024 * 1024);
        break;
      case "GB":
        bytes = (long) (number * 1024 * 1024 * 1024);
        break;
      case "TB":
        bytes = (long) (number * 1024L * 1024 * 1024 * 1024);
        break;
      case "PB":
        bytes = (long) (number * 1024L * 1024 * 1024 * 1024 * 1024);
        break;
      default:
        throw new IllegalArgumentException("Unsupported byte size unit: " + unit);
    }
  }

  /**
   * Returns the byte size in its canonical form (total bytes).
   *
   * @return The total number of bytes
   */
  public long getBytes() {
    return bytes;
  }

  @Override
  public Object value() {
    return bytes;
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", type().name());
    object.addProperty("value", bytes);
    object.addProperty("original", originalValue);
    return object;
  }

  @Override
  public String toString() {
    return originalValue;
  }
} 
