package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

public class TimeDuration implements Token {
  private final long nanoseconds;
  private final String original;

  public TimeDuration(String value) {
    this.original = value;
    String numPart = value.replaceAll("[^0-9.]", "");
    String unitPart = value.replaceAll("[0-9.]", "").toLowerCase();

    double base = Double.parseDouble(numPart);

    switch (unitPart) {
      case "ns":
        this.nanoseconds = (long) base;
        break;
      case "ms":
        this.nanoseconds = (long) (base * 1_000_000);
        break;
      case "s":
        this.nanoseconds = (long) (base * 1_000_000_000);
        break;
      case "m":
        this.nanoseconds = (long) (base * 60 * 1_000_000_000L);
        break;
      case "h":
        this.nanoseconds = (long) (base * 3600 * 1_000_000_000L);
        break;
      case "d":
        this.nanoseconds = (long) (base * 86400 * 1_000_000_000L);
        break;
      default:
        throw new IllegalArgumentException("Unknown unit in time duration: " + unitPart);
    }
  }

  public long getNanoseconds() {
    return this.nanoseconds;
  }

  @Override
  public String toString() {
    return original;
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(toString());
  }

  @Override
  public String value() {
    return toString();
  }

  @Override
  public TokenType type() {
    return TokenType.STRING; // or a new TokenType.TIMEDURATION
  }
}
