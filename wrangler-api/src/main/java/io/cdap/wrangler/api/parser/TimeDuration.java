package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class TimeDuration implements Token {
  private final String original;
  private final long millis;

  public TimeDuration(String value) {
    this.original = value.trim().toLowerCase();
    this.millis = parseMillis(this.original);
  }

  private long parseMillis(String str) {
    double number;
    if (str.endsWith("ms")) {
      number = Double.parseDouble(str.replace("ms", ""));
      return (long)(number);
    } else if (str.endsWith("s")) {
      number = Double.parseDouble(str.replace("s", ""));
      return (long)(number * 1000);
    } else if (str.endsWith("min")) {
      number = Double.parseDouble(str.replace("min", ""));
      return (long)(number * 60 * 1000);
    } else if (str.endsWith("h")) {
      number = Double.parseDouble(str.replace("h", ""));
      return (long)(number * 60 * 60 * 1000);
    } else {
      throw new IllegalArgumentException("Invalid time duration format: " + str);
    }
  }

  public long getMilliseconds() {
    return millis;
  }

  @Override
  public Object value() {
    return millis;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.TIME_DURATION.name());
    object.addProperty("value", millis);
    object.addProperty("original", original);
    return object;
  }
}
