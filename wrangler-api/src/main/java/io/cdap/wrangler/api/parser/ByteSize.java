/*
 * Copyright © 2024 Cask Data, Inc.
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
import io.cdap.wrangler.api.Token;
import io.cdap.wrangler.api.TokenType;

import java.util.Objects;

/**
 * Represents a byte size token in the Wrangler grammar.
 * This class implements the Token interface and provides functionality for
 * parsing and representing byte size values with their units.
 */
public class ByteSize implements Token {
  private final long value;
  private final String unit;
  private final String originalString;

  /**
   * Creates a new ByteSize token with the specified value and unit.
   *
   * @param value the numeric value of the byte size
   * @param unit the unit of the byte size (e.g., "B", "KB", "MB", "GB")
   * @param originalString the original string representation of the byte size
   */
  public ByteSize(long value, String unit, String originalString) {
    this.value = value;
    this.unit = unit;
    this.originalString = originalString;
  }

  /**
   * Returns the numeric value of the byte size.
   *
   * @return the value as a long
   */
  public long getValue() {
    return value;
  }

  /**
   * Returns the unit of the byte size.
   *
   * @return the unit as a string
   */
  public String getUnit() {
    return unit;
  }

  /**
   * Returns the original string representation of the byte size.
   *
   * @return the original string
   */
  public String getOriginalString() {
    return originalString;
  }

  /**
   * Converts the byte size to bytes.
   *
   * @return the byte size in bytes
   */
  public long toBytes() {
    switch (unit.toUpperCase()) {
      case "B":
        return value;
      case "KB":
        return value * 1024;
      case "MB":
        return value * 1024 * 1024;
      case "GB":
        return value * 1024 * 1024 * 1024;
      case "TB":
        return value * 1024L * 1024L * 1024L * 1024L;
      default:
        return value;
    }
  }

  @Override
  public Object value() {
    return toBytes();
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  @Override
  public JsonElement toJson() {
    JsonObject json = new JsonObject();
    json.addProperty("value", value);
    json.addProperty("unit", unit);
    json.addProperty("bytes", toBytes());
    json.addProperty("original", originalString);
    return json;
  }

  @Override
  public String toString() {
    return originalString;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ByteSize byteSize = (ByteSize) o;
    return value == byteSize.value && Objects.equals(unit, byteSize.unit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, unit);
  }
}

