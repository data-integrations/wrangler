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

/**
 * A class representing byte size values with units (e.g., "10KB", "1.5MB").
 * This token type is used for parsing storage amounts in various units.
 */
@PublicEvolving
public class ByteSize implements Token {
  /** Number of bytes in a kilobyte. */
  private static final long BYTES_PER_KB = 1024L;
  
  /** Number of bytes in a megabyte. */
  private static final long BYTES_PER_MB = BYTES_PER_KB * 1024L;
  
  /** Number of bytes in a gigabyte. */
  private static final long BYTES_PER_GB = BYTES_PER_MB * 1024L;
  
  /** Number of bytes in a terabyte. */
  private static final long BYTES_PER_TB = BYTES_PER_GB * 1024L;

  /** The size in bytes. */
  private final long bytes;

  /** The original input string. */
  private final String originalInput;

  /**
   * Creates a new ByteSize instance from a string representation.
   *
   * @param input The input string (e.g. "10KB", "1.5MB")
   * @throws IllegalArgumentException if the input format is invalid
   */
  public ByteSize(String input) {
    this.originalInput = input;
    this.bytes = parseSize(input);
  }

  /**
   * Parses the size string into bytes.
   *
   * @param size The size string to parse
   * @return The number of bytes
   * @throws IllegalArgumentException if the unit is unknown
   */
  private long parseSize(final String size) {
    String number = size.replaceAll("[^0-9.]", "");
    String unit = size.replaceAll("[0-9.]", "");
    double value = Double.parseDouble(number);
    
    switch (unit.toUpperCase()) {
      case "B":
        return (long) value;
      case "KB":
        return (long) (value * BYTES_PER_KB);
      case "MB":
        return (long) (value * BYTES_PER_MB);
      case "GB":
        return (long) (value * BYTES_PER_GB);
      case "TB":
        return (long) (value * BYTES_PER_TB);
      default:
        throw new IllegalArgumentException("Unknown unit: " + unit);
    }
  }

  /**
   * Gets the size in bytes.
   *
   * @return The number of bytes
   */
  public long getBytes() {
    return bytes;
  }

  /**
   * Gets the size in kilobytes.
   *
   * @return The number of kilobytes
   */
  public double getKilobytes() {
    return bytes / (double) BYTES_PER_KB;
  }

  /**
   * Gets the size in megabytes.
   *
   * @return The number of megabytes
   */
  public double getMegabytes() {
    return bytes / (double) BYTES_PER_MB;
  }

  /**
   * Gets the size in gigabytes.
   *
   * @return The number of gigabytes
   */
  public double getGigabytes() {
    return bytes / (double) BYTES_PER_GB;
  }

  /**
   * Gets the size in terabytes.
   *
   * @return The number of terabytes
   */
  public double getTerabytes() {
    return bytes / (double) BYTES_PER_TB;
  }

  @Override
  public String value() {
    return originalInput;
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.BYTE_SIZE.name());
    object.addProperty("value", bytes);
    object.addProperty("originalInput", originalInput);
    return object;
  }
}
