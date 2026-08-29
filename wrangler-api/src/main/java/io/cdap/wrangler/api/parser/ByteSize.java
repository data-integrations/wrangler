/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * Represents a ByteSize token, capable of parsing strings like "10KB", "1.5MB",
 * and converting them into bytes.
 */
@PublicEvolving
public class ByteSize implements Token {

  // Multipliers for each unit
  private static final double KILOBYTE = 1024.0;
  private static final double MEGABYTE = KILOBYTE * 1024.0;
  private static final double GIGABYTE = MEGABYTE * 1024.0;
  private static final double TERABYTE = GIGABYTE * 1024.0;

  // Parsed byte value stored as long
  private final long bytesValue;

  /**
   * Constructs a ByteSize token by parsing the given size string.
   *
   * @param sizeString The string to parse (e.g., "10KB", "1.5MB").
   * @throws IllegalArgumentException If the string format is invalid.
   */
  public ByteSize(String sizeString) {
    this.bytesValue = parseSize(sizeString);
  }

  /**
   * Parses a size string and converts it into bytes.
   *
   * @param sizeString The input string representing a byte size.
   * @return The size in bytes.
   */
  private long parseSize(String sizeString) {
    if (sizeString == null || sizeString.trim().isEmpty()) {
      throw new IllegalArgumentException("Size string must not be null or empty.");
    }

    sizeString = sizeString.trim().toUpperCase();
    String numericPart;
    double multiplier;

    try {
      if (sizeString.endsWith("KB")) {
        numericPart = sizeString.substring(0, sizeString.length() - 2);
        multiplier = KILOBYTE;
      } else if (sizeString.endsWith("MB")) {
        numericPart = sizeString.substring(0, sizeString.length() - 2);
        multiplier = MEGABYTE;
      } else if (sizeString.endsWith("GB")) {
        numericPart = sizeString.substring(0, sizeString.length() - 2);
        multiplier = GIGABYTE;
      } else if (sizeString.endsWith("TB")) {
        numericPart = sizeString.substring(0, sizeString.length() - 2);
        multiplier = TERABYTE;
      } else if (sizeString.endsWith("B")) {
        numericPart = sizeString.substring(0, sizeString.length() - 1);
        multiplier = 1.0;
      } else {
        throw new IllegalArgumentException("Invalid byte size format or unsupported unit in string: " + sizeString);
      }

      if (numericPart.isEmpty()) {
        throw new IllegalArgumentException("Missing numeric value in size string: " + sizeString);
      }

      double parsedValue = Double.parseDouble(numericPart);
      if (parsedValue < 0) {
        throw new IllegalArgumentException("Size value cannot be negative: " + sizeString);
      }

      return (long) (parsedValue * multiplier); // Truncate to long
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid numeric value in size string: " + sizeString, e);
    }
  }

  /**
   * @return Size in bytes.
   */
  public long getBytes() {
    return bytesValue;
  }

  /**
   * @return Size in kilobytes.
   */
  public double getKiloBytes() {
    return bytesValue / KILOBYTE;
  }

  /**
   * @return Size in megabytes.
   */
  public double getMegaBytes() {
    return bytesValue / MEGABYTE;
  }

  /**
   * @return Size in gigabytes.
   */
  public double getGigaBytes() {
    return bytesValue / GIGABYTE;
  }

  /**
   * @return Size in terabytes.
   */
  public double getTeraBytes() {
    return bytesValue / TERABYTE;
  }

  @Override
  public Object value() {
    return bytesValue;
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.BYTE_SIZE.name());
    object.addProperty("value", bytesValue);
    return object;
  }

  @Override
  public String toString() {
    return bytesValue + "B";
  }
}
