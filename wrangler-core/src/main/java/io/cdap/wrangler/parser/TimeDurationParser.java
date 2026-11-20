package io.cdap.wrangler.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses time durations like "5s", "10min", "2h", "3d", etc. into milliseconds.
 */
public class TimeDurationParser {
  private static final Pattern DURATION_PATTERN = Pattern.compile("(\\d+)\\s*(ms|s|m|h|d)");

  public long parse(String input) throws IllegalArgumentException {
    Matcher matcher = DURATION_PATTERN.matcher(input.trim().toLowerCase());
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid time duration: " + input);
    }

    long value = Long.parseLong(matcher.group(1));
    String unit = matcher.group(2);

    switch (unit) {
      case "ms":
        return value;
      case "s":
        return value * 1000;
      case "m":
        return value * 60 * 1000;
      case "h":
        return value * 60 * 60 * 1000;
      case "d":
        return value * 24 * 60 * 60 * 1000;
      default:
        throw new IllegalArgumentException("Unknown time unit: " + unit);
    }
  }
}
