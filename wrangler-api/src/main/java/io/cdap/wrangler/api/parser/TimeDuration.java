package io.cdap.wrangler.api.parser;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.util.Locale;

@PublicEvolving
public class TimeDuration implements Token {
  private final long milliseconds;
  private final String original;

  public TimeDuration(String input) {
    this.original = input;
    this.milliseconds = parseToMillis(input.trim().toLowerCase(Locale.ENGLISH));
  }

  private long parseToMillis(String input) {
    if (input.endsWith("ms")) return parseNumber(input, "ms");
    if (input.endsWith("s")) return parseNumber(input, "s") * 1000L;
    if (input.endsWith("m")) return parseNumber(input, "m") * 60L * 1000L;
    if (input.endsWith("h")) return parseNumber(input, "h") * 60L * 60L * 1000L;
    throw new IllegalArgumentException("Unsupported time unit: " + input);
  }

  private long parseNumber(String input, String suffix) {
    return (long) Double.parseDouble(input.substring(0, input.length() - suffix.length()));
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
  public JsonObject toJson() {
    JsonObject obj = new JsonObject();
    obj.add("original", new JsonPrimitive(original));
    obj.add("milliseconds", new JsonPrimitive(milliseconds));
    return obj;
  }
}
