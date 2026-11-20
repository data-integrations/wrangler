package io.cdap.wrangler.api.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A token representing a time duration with unit (e.g., 150ms, 2.5s).
 */
public class TimeDuration extends Token {
  private static final Pattern TIME_DURATION_PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)([mshd]|ms)");
  private final double originalValue;
  private final String originalUnit;
  private final long nanoseconds;

  /**
   * Constructor for creating a time duration token.
   *
   * @param value String representation of time duration (e.g., "150ms", "2.5s")
   */
  public TimeDuration(String value) {
    super(value, TokenType.TIME_DURATION);
    
    Matcher matcher = TIME_DURATION_PATTERN.matcher(value);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid time duration format: " + value);
    }
    
    this.originalValue = Double.parseDouble(matcher.group(1));
    this.originalUnit = matcher.group(2);
    this.nanoseconds = calculateNanoseconds(originalValue, originalUnit);
  }

  /**
   * Get the time duration in nanoseconds.
   *
   * @return Duration in nanoseconds
   */
  public long getNanoseconds() {
    return nanoseconds;
  }

  /**
   * Get the time duration in milliseconds.
   *
   * @return Duration in milliseconds
   */
  public double getMilliseconds() {
    return nanoseconds / 1_000_000.0;
  }

  /**
   * Get the time duration in seconds.
   *
   * @return Duration in seconds
   */
  public double getSeconds() {
    return nanoseconds / 1_000_000_000.0;
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
   * Get the original unit of the time duration.
   *
   * @return Original unit string (e.g., "ms", "s")
   */
  public String getOriginalUnit() {
    return originalUnit;
  }

  /**
   * Convert a value in a specific unit to nanoseconds.
   *
   * @param value Numeric value
   * @param unit Unit string (e.g., "ms", "s")
   * @return Value converted to nanoseconds
   */
  private long calculateNanoseconds(double value, String unit) {
    switch (unit) {
      case "ms":
        return (long) (value * 1_000_000);
      case "s":
        return (long) (value * 1_000_000_000);
      case "m":
        return (long) (value * 60 * 1_000_000_000L);
      case "h":
        return (long) (value * 60 * 60 * 1_000_000_000L);
      case "d":
        return (long) (value * 24 * 60 * 60 * 1_000_000_000L);
      default:
        throw new IllegalArgumentException("Unsupported time unit: " + unit);
    }
  }

  /**
   * Convert nanoseconds to a specified unit.
   *
   * @param targetUnit Unit to convert to (ms, s, m, h, d)
   * @return Value in the specified unit
   */
  public double convertTo(String targetUnit) {
    switch (targetUnit) {
      case "ms":
        return nanoseconds / 1_000_000.0;
      case "s":
        return nanoseconds / 1_000_000_000.0;
      case "m":
        return nanoseconds / (60.0 * 1_000_000_000L);
      case "h":
        return nanoseconds / (60.0 * 60 * 1_000_000_000L);
      case "d":
        return nanoseconds / (24.0 * 60 * 60 * 1_000_000_000L);
      default:
        throw new IllegalArgumentException("Unsupported target time unit: " + targetUnit);
    }
  }
}
