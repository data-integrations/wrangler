package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import io.cdap.wrangler.api.TokenType;

/**
 * Parses duration values such as "10s", "5min", "2h", "1d" into milliseconds.
 */
public class TimeDuration implements Token {
  private final String rawValue;
  private final long milliseconds;

  public TimeDuration(String value) {
    this.rawValue = value;
    this.milliseconds = parseToMilliseconds(value);
  }

  private long parseToMilliseconds(String input) {
    input = input.trim().toLowerCase();
    double number = Double.parseDouble(input.replaceAll("[^0-9.]", ""));

    if (input.endsWith("ms")) {
      return (long) number;
    } else if (input.endsWith("s")) {
      return (long) (number * 1000);
    } else if (input.endsWith("m") || input.endsWith("min")) {
      return (long) (number * 60 * 1000);
    } else if (input.endsWith("h")) {
      return (long) (number * 60 * 60 * 1000);
    } else if (input.endsWith("d")) {
      return (long) (number * 24 * 60 * 60 * 1000);
    } else {
      throw new IllegalArgumentException("Invalid time duration format: " + input);
    }
  }

  public long getMilliseconds() {
    return milliseconds;
  }

  @Override
  public Object value() {
    return milliseconds;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(milliseconds);
  }
}
