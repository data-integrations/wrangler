package io.cdap.wrangler.api.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A token representing a byte size value with unit (e.g., 10KB, 1.5MB).
 */
public class ByteSize extends Token {
  private static final Pattern BYTE_SIZE_PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)([kKmMgGtTpP][bB])");
  private final double originalValue;
  private final String originalUnit;
  private final long bytes;

  /**
   * Constructor for creating a byte size token.
   *
   * @param value String representation of byte size (e.g., "10KB", "1.5MB")
   */
  public ByteSize(String value) {
    super(value, TokenType.BYTE_SIZE);
    
    Matcher matcher = BYTE_SIZE_PATTERN.matcher(value);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid byte size format: " + value);
    }
    
    this.originalValue = Double.parseDouble(matcher.group(1));
    this.originalUnit = matcher.group(2);
    this.bytes = calculateBytes(originalValue, originalUnit);
  }

  /**
   * Get the byte size in bytes.
   *
   * @return Size in bytes
   */
  public long getBytes() {
    return bytes;
  }

  /**
   * Get the original value without unit conversion.
   *
   * @return Original numeric value
   */
  public double getOriginalValue() {
    return originalValue;
  }

  /**
   * Get the original unit of the byte size.
   *
   * @return Original unit string (e.g., "KB", "MB")
   */
  public String getOriginalUnit() {
    return originalUnit;
  }

  /**
   * Convert a value in a specific unit to bytes.
   *
   * @param value Numeric value
   * @param unit Unit string (e.g., "KB", "MB")
   * @return Value converted to bytes
   */
  private long calculateBytes(double value, String unit) {
    unit = unit.toLowerCase();
    switch (unit) {
      case "kb":
        return (long) (value * 1024);
      case "mb":
        return (long) (value * 1024 * 1024);
      case "gb":
        return (long) (value * 1024 * 1024 * 1024);
      case "tb":
        return (long) (value * 1024 * 1024 * 1024 * 1024);
      case "pb":
        return (long) (value * 1024 * 1024 * 1024 * 1024 * 1024);
      default:
        throw new IllegalArgumentException("Unsupported byte unit: " + unit);
    }
  }

  /**
   * Convert bytes to a specified unit.
   *
   * @param targetUnit Unit to convert to (kb, mb, gb, tb, pb)
   * @return Value in the specified unit
   */
  public double convertTo(String targetUnit) {
    targetUnit = targetUnit.toLowerCase();
    switch (targetUnit) {
      case "kb":
        return bytes / 1024.0;
      case "mb":
        return bytes / (1024.0 * 1024);
      case "gb":
        return bytes / (1024.0 * 1024 * 1024);
      case "tb":
        return bytes / (1024.0 * 1024 * 1024 * 1024);
      case "pb":
        return bytes / (1024.0 * 1024 * 1024 * 1024 * 1024);
      default:
        throw new IllegalArgumentException("Unsupported target byte unit: " + targetUnit);
    }
  }
}
