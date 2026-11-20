package io.cdap.wrangler.api.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A token that represents byte size like 10KB, 1.5MB, etc.
 */
public class ByteSize extends Token {
  private static final Pattern PATTERN = Pattern.compile("(?i)([\\d.]+)\\s*(B|KB|MB|GB|TB)");
  private final double size;
  private final String unit;

  public ByteSize(String value) {
    super(value);
    Matcher matcher = PATTERN.matcher(value.trim());
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid byte size format: " + value);
    }

    this.size = Double.parseDouble(matcher.group(1));
    this.unit = matcher.group(2).toUpperCase();
  }

  public long getBytes() {
    switch (unit) {
      case "B":
        return (long) size;
      case "KB":
        return (long) (size * 1024);
      case "MB":
        return (long) (size * 1024 * 1024);
      case "GB":
        return (long) (size * 1024 * 1024 * 1024);
      case "TB":
        return (long) (size * 1024L * 1024 * 1024 * 1024);
      default:
        throw new IllegalStateException("Unknown unit: " + unit);
    }
  }
}
