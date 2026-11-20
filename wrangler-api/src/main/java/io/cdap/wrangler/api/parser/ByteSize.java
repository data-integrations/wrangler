package io.cdap.wrangler.api.parser;

import com.google.gson.JsonPrimitive;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ByteSize implements Token {
  private static final Pattern PATTERN = Pattern.compile("([0-9]+(?:\\.[0-9]+)?)([kKmMgGtT]?[bB])");

  private final long bytes;
  private final String original;

  public ByteSize(String value) {
    this.original = value.trim();
    Matcher matcher = PATTERN.matcher(original);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid byte size format: " + value);
    }

    double number = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(2).toLowerCase();

    switch (unit) {
      case "b":
        bytes = (long) number;
        break;
      case "kb":
        bytes = (long) (number * 1024);
        break;
      case "mb":
        bytes = (long) (number * 1024 * 1024);
        break;
      case "gb":
        bytes = (long) (number * 1024 * 1024 * 1024);
        break;
      case "tb":
        bytes = (long) (number * 1024L * 1024 * 1024 * 1024);
        break;
      default:
        throw new IllegalArgumentException("Unsupported unit: " + unit);
    }
  }

  public long getBytes() {
    return bytes;
  }

  @Override
  public Object value() {
    return getBytes();
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  @Override
  public com.google.gson.JsonElement toJson() {
    return new JsonPrimitive(original);
  }
}
