package io.cdap.wrangler.api.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TimeDuration parser for token values like "100ms", "2s", "3m", "1h", etc.
 */
public class TimeDuration implements Token {
  private static final Pattern PATTERN =
      Pattern.compile("(?i)^(\\d+)(MS|S|M|H|D)$");

  private final String value;
  private long milliseconds;

  public TimeDuration(String value) throws ArgumentParseException {
    this.value = value.trim().toUpperCase();
    parse();
  }

  private void parse() throws ArgumentParseException {
    Matcher matcher = PATTERN.matcher(value);
    if (!matcher.matches()) {
      throw new ArgumentParseException(
          String.format("Invalid time duration format '%s'. Allowed formats: 100ms, 5s, 2m, 1h, 1d", value));
    }

    long number = Long.parseLong(matcher.group(1));
    String unit = matcher.group(2);

    switch (unit) {
      case "MS":
        milliseconds = number;
        break;
      case "S":
        milliseconds = number * 1000;
        break;
      case "M":
        milliseconds = number * 60 * 1000;
        break;
      case "H":
        milliseconds = number * 60 * 60 * 1000;
        break;
      case "D":
        milliseconds = number * 24 * 60 * 60 * 1000;
        break;
      default:
        throw new ArgumentParseException("Unknown time unit: " + unit);
    }
  }

  public long getMilliseconds() {
    return milliseconds;
  }

  @Override
  public String toString() {
    return String.valueOf(milliseconds);
  }
}
