package io.cdap.wrangler.api.parser;

import com.google.gson.JsonPrimitive;
import com.google.gson.JsonElement;

public class ByteSize implements Token {
  private final String value;
  private final long bytes;

  public ByteSize(String value) {
    this.value = value;
    value = value.trim().toUpperCase();

    if (value.endsWith("KB")) {
      bytes = (long)(Double.parseDouble(value.replace("KB", "")) * 1024);
    } else if (value.endsWith("MB")) {
      bytes = (long)(Double.parseDouble(value.replace("MB", "")) * 1024 * 1024);
    } else if (value.endsWith("GB")) {
      bytes = (long)(Double.parseDouble(value.replace("GB", "")) * 1024 * 1024 * 1024);
    } else if (value.endsWith("B")) {
      bytes = Long.parseLong(value.replace("B", ""));
    } else {
      throw new IllegalArgumentException("Invalid ByteSize format: " + value);
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
    return TokenType.BYTE_SIZE;
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(bytes);
  }
}
