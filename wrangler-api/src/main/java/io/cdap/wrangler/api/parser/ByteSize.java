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

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A token class representing byte sizes with various units (B, KB, MB, GB, TB).
 * Examples: "10KB", "1.5MB", "2GB"
 */
public class ByteSize extends Token {
  private static final Pattern PATTERN = Pattern.compile("^(\\d+(?:\\.\\d+)?)\\s*((?:k|m|g|t)?b)$", Pattern.CASE_INSENSITIVE);
  
  private final double value;
  private final String unit;
  private final long bytes;

  /**
   * Constructor to parse a byte size string.
   *
   * @param byteSize The string representation of a byte size (e.g., "10KB", "1.5MB").
   * @throws TokenException If the string cannot be parsed as a valid byte size.
   */
  public ByteSize(String byteSize) throws TokenException {
    super(byteSize);
    
    if (byteSize == null || byteSize.isEmpty()) {
      throw new TokenException("Byte size cannot be null or empty");
    }
    
    try {
      Matcher matcher = PATTERN.matcher(byteSize.trim());
      if (!matcher.matches()) {
        throw new TokenException(String.format("Invalid byte size format: '%s'. Expected format: '10KB', '1.5MB', etc.", byteSize));
      }
      
      this.value = Double.parseDouble(matcher.group(1));
      this.unit = matcher.group(2).toLowerCase(Locale.ENGLISH);
      this.bytes = convertToBytes(this.value, this.unit);
    } catch (NumberFormatException e) {
      throw new TokenException(String.format("Failed to parse numeric part of byte size: '%s'", byteSize), e);
    }
  }
  
  /**
   * Gets the numeric value part of the byte size.
   *
   * @return The numeric value.
   */
  public double getValue() {
    return value;
  }
  
  /**
   * Gets the unit part of the byte size.
   *
   * @return The unit string (e.g., "kb", "mb", etc.).
   */
  public String getUnit() {
    return unit;
  }
  
  /**
   * Gets the byte size converted to bytes.
   *
   * @return The size in bytes.
   */
  public long getBytes() {
    return bytes;
  }
  
  /**
   * Gets the byte size converted to kilobytes.
   *
   * @return The size in kilobytes.
   */
  public double getKilobytes() {
    return bytes / 1024.0;
  }
  
  /**
   * Gets the byte size converted to megabytes.
   *
   * @return The size in megabytes.
   */
  public double getMegabytes() {
    return bytes / (1024.0 * 1024.0);
  }
  
  /**
   * Gets the byte size converted to gigabytes.
   *
   * @return The size in gigabytes.
   */
  public double getGigabytes() {
    return bytes / (1024.0 * 1024.0 * 1024.0);
  }
  
  /**
   * Gets the byte size converted to terabytes.
   *
   * @return The size in terabytes.
   */
  public double getTerabytes() {
    return bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0);
  }
  
  /**
   * Converts a value with unit to bytes.
   *
   * @param value The numeric value.
   * @param unit The unit (e.g., "b", "kb", "mb", etc.).
   * @return The equivalent size in bytes.
   * @throws TokenException If the unit is not recognized.
   */
  private long convertToBytes(double value, String unit) throws TokenException {
    if (unit.equals("b")) {
      return (long) value;
    } else if (unit.equals("kb")) {
      return (long) (value * 1024);
    } else if (unit.equals("mb")) {
      return (long) (value * 1024 * 1024);
    } else if (unit.equals("gb")) {
      return (long) (value * 1024 * 1024 * 1024);
    } else if (unit.equals("tb")) {
      return (long) (value * 1024 * 1024 * 1024 * 1024);
    } else {
      throw new TokenException(String.format("Unrecognized byte size unit: '%s'", unit));
    }
  }
  
  @Override
  public TokenType getType() {
    return TokenType.BYTE_SIZE;
  }
}