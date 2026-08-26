/*
 * Copyright © 2017-2022 Cask Data, Inc.
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * This class represents a byte size value with a unit (KB, MB, GB, etc.).
 * It provides methods to parse string representations of byte sizes and
 * convert between different units.
 */
@PublicEvolving
public class ByteSize implements Token {
  // Pattern to match formats like "10KB", "1.5MB", etc.
  private static final Pattern BYTE_SIZE_PATTERN = 
      Pattern.compile("^(\\d+(\\.\\d+)?)\\s*(B|KB|MB|GB|TB|PB|EB|ZB|YB)$", Pattern.CASE_INSENSITIVE);
  
  private final String original;
  private final double value;
  private final ByteUnit unit;
  
  /**
   * Enumeration of byte units with conversion factors.
   * Uses binary (powers of 1024) conversion factors, which is the standard for disk/memory sizes.
   */
  public enum ByteUnit {
    B(1L),
    KB(1024L),
    MB(1024L * 1024L),
    GB(1024L * 1024L * 1024L),
    TB(1024L * 1024L * 1024L * 1024L),
    PB(1024L * 1024L * 1024L * 1024L * 1024L),
    EB(1024L * 1024L * 1024L * 1024L * 1024L * 1024L),
    ZB(1024L * 1024L * 1024L * 1024L * 1024L * 1024L * 1024L),
    YB(1024L * 1024L * 1024L * 1024L * 1024L * 1024L * 1024L * 1024L);
    
    private final long bytesFactor;
    
    ByteUnit(long bytesFactor) {
      this.bytesFactor = bytesFactor;
    }
    
    public long getBytesFactor() {
      return bytesFactor;
    }
    
    /**
     * Converts a value from this unit to the specified unit.
     *
     * @param value The value to convert
     * @param targetUnit The target unit
     * @return The converted value
     */
    public double convertTo(double value, ByteUnit targetUnit) {
      if (this == targetUnit) {
        return value;
      }
      
      // Convert to bytes first, then to the target unit
      double bytes = value * this.bytesFactor;
      return bytes / targetUnit.bytesFactor;
    }
    
    /**
     * Gets a ByteUnit from a string representation.
     *
     * @param unitString The string representation of the unit (e.g., "KB", "MB", etc.)
     * @return The corresponding ByteUnit
     * @throws IllegalArgumentException if the unit is not recognized
     */
    public static ByteUnit fromString(String unitString) {
      try {
        return ByteUnit.valueOf(unitString.toUpperCase(Locale.ENGLISH));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Unrecognized byte unit: " + unitString);
      }
    }
  }
  
  /**
   * Constructs a ByteSize from a string representation like "10KB", "1.5MB", etc.
   *
   * @param byteSizeString The string representation of the byte size
   * @throws IllegalArgumentException if the string is not a valid byte size
   */
  public ByteSize(String byteSizeString) {
    this.original = byteSizeString;
    Matcher matcher = BYTE_SIZE_PATTERN.matcher(byteSizeString);
    
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid byte size format: " + byteSizeString);
    }
    
    this.value = Double.parseDouble(matcher.group(1));
    this.unit = ByteUnit.fromString(matcher.group(3));
  }
  
  /**
   * Constructs a ByteSize with specific value and unit.
   *
   * @param value The numeric value
   * @param unit The byte unit
   */
  public ByteSize(double value, ByteUnit unit) {
    this.original = value + unit.name();
    this.value = value;
    this.unit = unit;
  }
  
  /**
   * Returns the original string representation.
   *
   * @return The original string
   */
  public String getOriginal() {
    return original;
  }
  
  /**
   * Returns the numeric value in the original unit.
   *
   * @return The numeric value
   */
  public double getValue() {
    return value;
  }
  
  /**
   * Returns the unit of this byte size.
   *
   * @return The byte unit
   */
  public ByteUnit getUnit() {
    return unit;
  }
  
  /**
   * Returns the total number of bytes represented by this byte size.
   *
   * @return The total bytes as a long value
   */
  public long getBytes() {
    return (long) (value * unit.getBytesFactor());
  }
  
  /**
   * Converts this byte size to a different unit.
   *
   * @param targetUnit The target unit to convert to
   * @return The converted value
   */
  public double convertTo(ByteUnit targetUnit) {
    return unit.convertTo(value, targetUnit);
  }
  
  /**
   * Converts this byte size to a new ByteSize object in a different unit.
   *
   * @param targetUnit The target unit to convert to
   * @return A new ByteSize object in the target unit
   */
  public ByteSize to(ByteUnit targetUnit) {
    if (unit == targetUnit) {
      return this;
    }
    return new ByteSize(convertTo(targetUnit), targetUnit);
  }
  
  /**
   * Formats this byte size with the given precision.
   *
   * @param precision Number of decimal digits to include
   * @return A formatted string representation
   */
  public String format(int precision) {
    String format = "%." + precision + "f%s";
    return String.format(format, value, unit.name());
  }
  
  /**
   * Creates a ByteSize object from a number of bytes.
   *
   * @param bytes The number of bytes
   * @return A ByteSize object representing the specified number of bytes
   */
  public static ByteSize fromBytes(long bytes) {
    return new ByteSize(bytes, ByteUnit.B);
  }
  
  /**
   * Parses a byte size string, with null handling.
   *
   * @param byteSizeString The string to parse
   * @return A ByteSize object or null if the input was null
   * @throws IllegalArgumentException if the string is not a valid byte size
   */
  @Nullable
  public static ByteSize parse(@Nullable String byteSizeString) {
    if (byteSizeString == null || byteSizeString.trim().isEmpty()) {
      return null;
    }
    return new ByteSize(byteSizeString);
  }
  
  @Override
  public String toString() {
    return original;
  }
  
  @Override
  public ByteSize value() {
    return this;
  }
  
  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }
  
  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.BYTE_SIZE.name());
    object.addProperty("value", original);
    object.addProperty("bytes", getBytes());
    object.addProperty("unit", unit.name());
    return object;
  }
} 
