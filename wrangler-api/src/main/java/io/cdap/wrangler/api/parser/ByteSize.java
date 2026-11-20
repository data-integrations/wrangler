package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import io.cdap.wrangler.api.TokenType;

public class ByteSize implements Token {
  private final String rawValue;
  private final long bytes;

  public ByteSize(String value) {
    this.rawValue = value;
    this.bytes = parseToBytes(value);
  }

  private long parseToBytes(String input) {
    input = input.trim().toUpperCase();
    double number = Double.parseDouble(input.replaceAll("[^0-9.]", ""));
    if (input.endsWith("KB")) {
      return (long) (number * 1024);
    } else if (input.endsWith("MB")) {
      return (long) (number * 1024 * 1024);
    } else if (input.endsWith("GB")) {
      return (long) (number * 1024 * 1024 * 1024);
    } else if (input.endsWith("TB")) {
      return (long) (number * 1024L * 1024 * 1024 * 1024);
    } else if (input.endsWith("B")) {
      return (long) number;
    } else {
      throw new IllegalArgumentException("Invalid byte size format: " + input);
    }
  }

  public long getBytes() {
    return bytes;
  }

  @Override
  public Object value() {
    return bytes;
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE; // You must add this to the TokenType enum if it's not there yet
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(bytes);
  }
}
