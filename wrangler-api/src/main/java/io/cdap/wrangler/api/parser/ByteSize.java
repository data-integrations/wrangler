package io.cdap.wrangler.api.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonElement;

public class ByteSize implements Token {
  private static final Pattern PATTERN = Pattern.compile("(?i)(\\d+(\\.\\d+)?)([KMGTP]B)");
  private final double value;
  private final String unit;

  public ByteSize(String input) {
    Matcher matcher = PATTERN.matcher(input.trim());
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid byte size format: " + input);
    }
    this.value = Double.parseDouble(matcher.group(1));
    this.unit = matcher.group(3).toUpperCase();
  }

  public long getBytes() {
    switch (unit) {
      case "KB": return (long)(value * 1024L);
      case "MB": return (long)(value * 1024L * 1024L);
      case "GB": return (long)(value * 1024L * 1024L * 1024L);
      case "TB": return (long)(value * 1024L * 1024L * 1024L * 1024L);
      case "PB": return (long)(value * 1024L * 1024L * 1024L * 1024L * 1024L);
      default: throw new IllegalArgumentException("Unknown byte unit: " + unit);
    }
  }

  public double getValue() {
    return value;
  }

  public String getUnit() {
    return unit;
  }

  public static Pattern getPattern() {
    return PATTERN;
  }

  @Override
  public String toString() {
    return value + unit;
  }

  // ✅ Optional: Add a main method for quick testing
  public static void main(String[] args) {
    ByteSize size = new ByteSize("1.5GB");
    System.out.println("Original: " + size);
    System.out.println("Bytes: " + size.getBytes());
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
