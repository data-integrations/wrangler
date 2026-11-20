/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class representing byte size values with units (B, KB, MB, GB, TB, PB).
 */
@PublicEvolving
public class ByteSize implements Token {
  private static final Pattern BYTE_SIZE_PATTERN = 
      Pattern.compile("([0-9]+(?:\\.[0-9]+)?)\\s*([bkmgtp]b?)", Pattern.CASE_INSENSITIVE);
  private static final long BYTES_IN_KB = 1024L;
  private static final long BYTES_IN_MB = BYTES_IN_KB * 1024L;
  private static final long BYTES_IN_GB = BYTES_IN_MB * 1024L;
  private static final long BYTES_IN_TB = BYTES_IN_GB * 1024L;
  private static final long BYTES_IN_PB = BYTES_IN_TB * 1024L;

  private final String rawValue;
  private final double numericValue;
  private final String unit;
  private final long bytes;

  /**
   * Constructor for ByteSize.
   *
   * @param value String representation of byte size (e.g., "10MB", "1.5GB")
   */
  public ByteSize(String value) {
    this.rawValue = value;
    
    Matcher matcher = BYTE_SIZE_PATTERN.matcher(value.trim());
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid byte size format: " + value);
    }
    
    this.numericValue = Double.parseDouble(matcher.group(1));
    this.unit = matcher.group(2).toLowerCase();
    this.bytes = calculateBytes(numericValue, unit);
  }

  /**
   * Calculate bytes based on the numeric value and unit.
   *
   * @param value Numeric value
   * @param unit Unit (b, kb, mb, gb, tb, pb)
   * @return Number of bytes
   */
  private long calculateBytes(double value, String unit) {
    switch (unit.toLowerCase()) {
      case "b":
        return (long) value;
      case "kb":
      case "k":
        return (long) (value * BYTES_IN_KB);
      case "mb":
      case "m":
        return (long) (value * BYTES_IN_MB);
      case "gb":
      case "g":
        return (long) (value * BYTES_IN_GB);
      case "tb":
      case "t":
        return (long) (value * BYTES_IN_TB);
      case "pb":
      case "p":
        return (long) (value * BYTES_IN_PB);
      default:
        throw new IllegalArgumentException("Unsupported byte unit: " + unit);
    }
  }

  /**
   * Gets the raw string value.
   *
   * @return Raw string value
   */
  @Override
  public String value() {
    return rawValue;
  }

  /**
   * Gets the numeric part of the byte size.
   *
   * @return Numeric value
   */
  public double getNumericValue() {
    return numericValue;
  }

  /**
   * Gets the unit part of the byte size.
   *
   * @return Unit string
   */
  public String getUnit() {
    return unit;
  }

  /**
   * Gets the value converted to bytes.
   *
   * @return Number of bytes
   */
  public long getBytes() {
    return bytes;
  }

  /**
   * Gets the value converted to kilobytes.
   *
   * @return Number of kilobytes
   */
  public double getKilobytes() {
    return bytes / (double) BYTES_IN_KB;
  }

  /**
   * Gets the value converted to megabytes.
   *
   * @return Number of megabytes
   */
  public double getMegabytes() {
    return bytes / (double) BYTES_IN_MB;
  }

  /**
   * Gets the value converted to gigabytes.
   *
   * @return Number of gigabytes
   */
  public double getGigabytes() {
    return bytes / (double) BYTES_IN_GB;
  }

  /**
   * Gets the value converted to terabytes.
   *
   * @return Number of terabytes
   */
  public double getTerabytes() {
    return bytes / (double) BYTES_IN_TB;
  }

  /**
   * Gets the value converted to petabytes.
   *
   * @return Number of petabytes
   */
  public double getPetabytes() {
    return bytes / (double) BYTES_IN_PB;
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.BYTE_SIZE.name());
    object.addProperty("value", rawValue);
    object.addProperty("bytes", bytes);
    return object;
  }

  @Override
  public String toString() {
    return rawValue;
  }
}

