package io.cdap.wrangler.api.parser;

import com.google.gson.JsonPrimitive;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimeDuration implements Token {
  private static final Pattern PATTERN = Pattern.compile("([0-9]+(?:\\.[0-9]+)?)(ms|s|sec|m|min)");

  private final long milliseconds;
  private final String original;

  public TimeDuration(String value) {
    this.original = value.trim();
    Matcher matcher = PATTERN.matcher(original);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid time duration format: " + value);
    }

    double number = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(2).toLowerCase();

    switch (unit) {
      case "ms":
        milliseconds = (long) number;
        break;
      case "s":
      case "sec":
        milliseconds = (long) (number * 1000);
        break;
      case "m":
      case "min":
        milliseconds = (long) (number * 60 * 1000);
        break;
      default:
        throw new IllegalArgumentException("Unsupported unit: " + unit);
    }
  }

  public long getMilliseconds() {
    return milliseconds;
  }

  @Override
  public Object value() {
    return getMilliseconds();
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public com.google.gson.JsonElement toJson() {
    return new JsonPrimitive(original);
  }
}
