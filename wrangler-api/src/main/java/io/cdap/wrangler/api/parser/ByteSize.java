/*
 * Copyright © 2023-2025 Cask Data, Inc.
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

import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class representing byte sizes in tokens for data wrangling directives.
 * Supports parsing values with units like KB, MB, GB, and TB.
 */
@PublicEvolving
public class ByteSize implements Token {
  // Pattern to match a number followed by a byte unit (KB, MB, GB, TB)
  private static final Pattern BYTE_SIZE_PATTERN = 
      Pattern.compile("^(\\d+(?:\\.\\d+)?)([kKmMgGtT][bB])$");
  
  private final String originalValue;
  private final double rawValue;
  private final String unit;
  private final long bytes;

  /**
   * Creates a new ByteSize by parsing a string value.
   *
   * @param value String value like "10KB", "1.5MB", "2GB", etc.
   * @throws ParseException if the string value cannot be parsed
   */
  public ByteSize(String value) throws ParseException {
    this.originalValue = value;
    
    Matcher matcher = BYTE_SIZE_PATTERN.matcher(value);
    if (!matcher.matches()) {
      throw new ParseException("Invalid byte size format: " + value, 0);
    }
    
    this.rawValue = Double.parseDouble(matcher.group(1));
    if (rawValue < 0) {
      throw new ParseException("Byte size cannot be negative: " + value, 0);
    }
    
    this.unit = matcher.group(2).toUpperCase();
    this.bytes = convertToBytes(rawValue, unit);
  }
  
  /**
   * Returns the size in bytes.
   *
   * @return number of bytes
   */
  public long getBytes() {
    return bytes;
  }
  
  /**
   * Returns the size in kilobytes.
   *
   * @return number of kilobytes
   */
  public double getKilobytes() {
    return bytes / 1024.0;
  }
  
  /**
   * Returns the size in megabytes.
   *
   * @return number of megabytes
   */
  public double getMegabytes() {
    return bytes / (1024.0 * 1024.0);
  }
  
  /**
   * Returns the size in gigabytes.
   *
   * @return number of gigabytes
   */
  public double getGigabytes() {
    return bytes / (1024.0 * 1024.0 * 1024.0);
  }
  
  /**
   * Returns the size in terabytes.
   *
   * @return number of terabytes
   */
  public double getTerabytes() {
    return bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0);
  }
  
  @Override
  public String value() {
    return originalValue;
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.BYTE_SIZE.name());
    object.addProperty("value", originalValue);
    object.addProperty("bytes", bytes);
    return object;
  }
  
  private long convertToBytes(double value, String unit) {
    switch (unit) {
      case "KB":
        return (long) (value * 1024);
      case "MB":
        return (long) (value * 1024 * 1024);
      case "GB":
        return (long) (value * 1024 * 1024 * 1024);
      case "TB":
        return (long) (value * 1024 * 1024 * 1024 * 1024);
      default:
        // Should not reach here because of regex validation
        return (long) value;
    }
  }
}
