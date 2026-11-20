package io.cdap.wrangler.api.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonElement;

/**
 * Represents a time duration string like "500ms", "2h", or "1.5d"
 * and provides conversion to milliseconds.
 */
public class TimeDuration implements Token {
  private static final Pattern PATTERN = Pattern.compile("(?i)(\\d+(\\.\\d+)?)(ms|s|m|h|d)");
  private final double value;
  private final String unit;

  public TimeDuration(String input) {
    Matcher matcher = PATTERN.matcher(input.trim());
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid time duration format: " + input);
    }
    this.value = Double.parseDouble(matcher.group(1));
    this.unit = matcher.group(3).toLowerCase();
  }

  /**
   * Returns the time duration in milliseconds.
   */
  public long getMilliseconds() {
    switch (unit) {
      case "ms": return (long)(value);
      case "s": return (long)(value * 1000L);
      case "m": return (long)(value * 60L * 1000L);
      case "h": return (long)(value * 60L * 60L * 1000L);
      case "d": return (long)(value * 24L * 60L * 60L * 1000L);
      default: throw new IllegalArgumentException("Unknown time unit: " + unit);
    }
  }

  public static Pattern getPattern() {
    return PATTERN;
  }

  public double getValue() {
    return value;
  }

  public String getUnit() {
    return unit;
  }

  @Override
  public String toString() {
    return value + unit;
  }

  // ✅ Optional: Add a main method for testing
  public static void main(String[] args) {
    TimeDuration duration = new TimeDuration("2.5h");
    System.out.println("Original: " + duration);
    System.out.println("Milliseconds: " + duration.getMilliseconds());
  }

  @Override
  public Object value() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'value'");
  }

  @Override
  public TokenType type() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'type'");
  }

  @Override
  public JsonElement toJson() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'toJson'");
  }
}
