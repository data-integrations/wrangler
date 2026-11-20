package io.cdap.wrangler.api.parser;

import com.google.gson.JsonPrimitive;
import com.google.gson.JsonElement;

public class TimeDuration implements Token {
  private final String originalValue;
  private final long millis;

  public TimeDuration(String value) {
    this.originalValue = value;
    value = value.trim().toLowerCase();

    if (value.endsWith("ms")) {
      millis = Long.parseLong(value.replace("ms", ""));
    } else if (value.endsWith("s")) {
      millis = (long)(Double.parseDouble(value.replace("s", "")) * 1000);
    } else if (value.endsWith("m")) {
      millis = (long)(Double.parseDouble(value.replace("m", "")) * 60 * 1000);
    } else if (value.endsWith("h")) {
      millis = (long)(Double.parseDouble(value.replace("h", "")) * 60 * 60 * 1000);
    } else {
      throw new IllegalArgumentException("Invalid TimeDuration format: " + value);
    }
  }

  public long getMillis() {
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
    return new JsonPrimitive(millis);
  }
}
