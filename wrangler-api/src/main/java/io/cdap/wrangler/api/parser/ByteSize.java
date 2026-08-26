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
import com.google.gson.JsonPrimitive;
import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * A token implementation for byte size values.
 * Supports parsing of values like "10KB", "1.5MB", "2GB", etc.
 */
@PublicEvolving
public class ByteSize implements Token {
  private static final long serialVersionUID = 1L;
  private final String rawValue;
  private final long bytes;

  /**
   * Constructor for ByteSize token.
   *
   * @param value String representation of byte size (e.g., "10KB", "1.5MB")
   * @throws IllegalArgumentException if the value format is invalid
   */
  public ByteSize(String value) {
    this.rawValue = value;
    this.bytes = parseBytes(value);
  }

  /**
   * Parses a string representation of byte size into bytes.
   *
   * @param value String to parse
   * @return number of bytes
   * @throws IllegalArgumentException if the format is invalid
   */
  private long parseBytes(String value) {
    value = value.trim();
    String number = value.replaceAll("[^0-9.]", "");
    String unit = value.replaceAll("[0-9.]", "").toUpperCase();
    try {
      double amount = Double.parseDouble(number);
      switch (unit) {
        case "KB":
          return (long) (amount * 1024);
        case "MB":
          return (long) (amount * 1024 * 1024);
        case "GB":
          return (long) (amount * 1024 * 1024 * 1024);
        case "TB":
          return (long) (amount * 1024 * 1024 * 1024 * 1024);
        case "B":
          return (long) amount;
        default:
          throw new IllegalArgumentException("Invalid byte unit: " + unit);
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid byte size format: " + value);
    }
  }

  @Override
  public Object value() {
    return rawValue;
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(rawValue);
  }

  /**
   * Get the value in bytes.
   * @return number of bytes
   */
  public long getBytes() {
    return bytes;
  }

  /**
   * Get the value in kilobytes.
   * @return number of kilobytes
   */
  public double getKilobytes() {
    return bytes / 1024.0;
  }

  /**
   * Get the value in megabytes.
   * @return number of megabytes
   */
  public double getMegabytes() {
    return bytes / (1024.0 * 1024.0);
  }

  /**
   * Get the value in gigabytes.
   * @return number of gigabytes
   */
  public double getGigabytes() {
    return bytes / (1024.0 * 1024.0 * 1024.0);
  }
}



