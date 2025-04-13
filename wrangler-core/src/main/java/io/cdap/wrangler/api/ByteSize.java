package io.cdap.wrangler.api.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ByteSize parser for token values like "10KB", "5MB", etc.
 */
public class ByteSize implements Token {
  private static final Pattern PATTERN =
      Pattern.compile("(?i)^(\\d+)(B|KB|MB|GB|TB)$");

  private final String value;
  private long bytes;

  public ByteSize(String value) throws ArgumentParseException {
    this.value = value.trim().toUpperCase();
    parse();
  }

  private void parse() throws ArgumentParseException {
    Matcher matcher = PATTERN.matcher(value);
    if (!matcher.matches()) {
      throw new ArgumentParseException(
          String.format("Invalid byte size format '%s'. Allowed formats: 10KB, 2MB, 1GB, etc.", value));
    }

    long number = Long.parseLong(matcher.group(1));
    String unit = matcher.group(2);

    switch (unit) {
      case "B":
        bytes = number;
        break;
      case "KB":
        bytes = number * 1024;
        break;
      case "MB":
        bytes = number * 1024 * 1024;
        break;
      case "GB":
        bytes = number * 1024 * 1024 * 1024;
        break;
      case "TB":
        bytes = number * 1024L * 1024 * 1024 * 1024;
        break;
      default:
        throw new ArgumentParseException("Unknown size unit: " + unit);
    }
  }

  public long getBytes() {
    return bytes;
  }

  @Override
  public String toString() {
    return String.valueOf(bytes);
  }
}
