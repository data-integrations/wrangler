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

package io.cdap.wrangler.api.parser.token;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenType;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ByteSize class represents a size value with a byte unit (B, KB, MB, GB, TB, PB).
 * It parses a string representation of a byte size and provides methods to retrieve
 * the value in different units.
 */
public class ByteSize implements Token {
  private static final Pattern PATTERN = Pattern.compile("([\\d.]+)\\s*([kKmMgGtTpP]?[bB])");
  // Binary multiplier constants
  private static final long KILOBYTE = 1024L;
  private static final long MEGABYTE = KILOBYTE * KILOBYTE;
  private static final long GIGABYTE = MEGABYTE * KILOBYTE;
  private static final long TERABYTE = GIGABYTE * KILOBYTE;
  private static final long PETABYTE = TERABYTE * KILOBYTE;
  
  private final String value;
  private final double numValue;
  private final String unit;
  private final long bytes;

  /**
   * Constructor to create a ByteSize from a string.
   *
   * @param value String representation of the byte size, e.g. "10KB", "1.5MB"
   * @throws IllegalArgumentException If the input string doesn't match the expected format
   */
  public ByteSize(String value) {
    this.value = value;
    Matcher matcher = PATTERN.matcher(value);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
        String.format("Invalid byte size format: %s. Expected format: <number><unit>, e.g. 10KB", value));
    }
    
    this.numValue = Double.parseDouble(matcher.group(1));
    this.unit = matcher.group(2).toUpperCase(Locale.ENGLISH);
    this.bytes = calculateBytes(this.numValue, this.unit);
  }

  /**
   * Calculates the equivalent bytes from a value and unit.
   *
   * @param value Numeric value
   * @param unit Unit string (B, KB, MB, GB, TB, PB)
   * @return Number of bytes
   */
  private long calculateBytes(double value, String unit) {
    // Using binary prefixes (1 KB = 1024 bytes)
    switch (unit.toUpperCase(Locale.ENGLISH)) {
      case "B":
        return (long) value;
      case "KB":
        return (long) (value * KILOBYTE);
      case "MB":
        return (long) (value * MEGABYTE);
      case "GB":
        return (long) (value * GIGABYTE);
      case "TB":
        return (long) (value * TERABYTE);
      case "PB":
        return (long) (value * PETABYTE);
      default:
        throw new IllegalArgumentException("Unknown byte unit: " + unit);
    }
  }

  /**
   * Returns the original string value of this token.
   *
   * @return the original string value
   */
  @Override
  public Object value() {
    return value;
  }

  /**
   * Returns the token type - BYTE_SIZE.
   *
   * @return TokenType.BYTE_SIZE
   */
  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  /**
   * Converts this object to a JSON representation.
   *
   * @return JsonElement representing this object
   */
  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", type().name());
    object.addProperty("value", value);
    object.addProperty("bytes", bytes);
    return object;
  }

  /**
   * Returns the byte value.
   *
   * @return The size in bytes
   */
  public long getBytes() {
    return bytes;
  }

  /**
   * Returns the size in kilobytes.
   *
   * @return The size in kilobytes
   */
  public double getKilobytes() {
    return bytes / (double) KILOBYTE;
  }

  /**
   * Returns the size in megabytes.
   *
   * @return The size in megabytes
   */
  public double getMegabytes() {
    return bytes / (double) MEGABYTE;
  }

  /**
   * Returns the size in gigabytes.
   *
   * @return The size in gigabytes
   */
  public double getGigabytes() {
    return bytes / (double) GIGABYTE;
  }

  /**
   * Returns the size in terabytes.
   *
   * @return The size in terabytes
   */
  public double getTerabytes() {
    return bytes / (double) TERABYTE;
  }

  /**
   * Returns the size in petabytes.
   *
   * @return The size in petabytes
   */
  public double getPetabytes() {
    return bytes / (double) PETABYTE;
  }

  /**
   * Returns the numeric value parsed from the original string.
   *
   * @return The numeric value
   */
  public double getNumericValue() {
    return numValue;
  }

  /**
   * Returns the unit parsed from the original string.
   *
   * @return The unit string (B, KB, MB, GB, TB, PB)
   */
  public String getUnit() {
    return unit;
  }

  @Override
  public String toString() {
    return value;
  }
}
