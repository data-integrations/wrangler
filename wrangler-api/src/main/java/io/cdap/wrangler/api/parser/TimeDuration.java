package io.cdap.wrangler.api.parser;

import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonElement;

@PublicEvolving
public class TimeDuration implements Token {
  private static final Pattern TIME_PATTERN = Pattern.compile("([0-9]+(\\.[0-9]+)?)([A-Za-z]+)");
  private final long nanoseconds;

  public TimeDuration(String value) {
    super();
    Matcher matcher = TIME_PATTERN.matcher(value);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid time duration format: " + value);
    }

    double time = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(3).toUpperCase();

    switch (unit) {
      case "NS": nanoseconds = (long) time; break;
      case "MS": nanoseconds = (long) (time * 1_000_000); break;
      case "S": nanoseconds = (long) (time * 1_000_000_000); break;
      case "M": nanoseconds = (long) (time * 60 * 1_000_000_000); break;
      case "H": nanoseconds = (long) (time * 60 * 60 * 1_000_000_000); break;
      case "D": nanoseconds = (long) (time * 24 * 60 * 60 * 1_000_000_000); break;
      default: throw new IllegalArgumentException("Unknown time unit: " + unit);
    }
  }

  public long getNanoseconds() {
    return nanoseconds;
  }

  @Override
  public Object value() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'value'");
  }

  @Override
  public TokenType type() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'type'");
  }

  @Override
  public JsonElement toJson() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'toJson'");
  }
}