package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ByteSize implements Token {
  private final String original;
  private final long bytes;

  public ByteSize(String value) {
    this.original = value.trim().toLowerCase();
    this.bytes = parseBytes(this.original);
  }

  private long parseBytes(String str) {
    double number;
    if (str.endsWith("kb")) {
      number = Double.parseDouble(str.replace("kb", ""));
      return (long)(number * 1024);
    } else if (str.endsWith("mb")) {
      number = Double.parseDouble(str.replace("mb", ""));
      return (long)(number * 1024 * 1024);
    } else if (str.endsWith("gb")) {
      number = Double.parseDouble(str.replace("gb", ""));
      return (long)(number * 1024 * 1024 * 1024);
    } else if (str.endsWith("tb")) {
      number = Double.parseDouble(str.replace("tb", ""));
      return (long)(number * 1024L * 1024 * 1024 * 1024);
    } else {
      throw new IllegalArgumentException("Invalid byte size format: " + str);
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
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.BYTE_SIZE.name());
    object.addProperty("value", bytes);
    object.addProperty("original", original);
    return object;
  }
}
