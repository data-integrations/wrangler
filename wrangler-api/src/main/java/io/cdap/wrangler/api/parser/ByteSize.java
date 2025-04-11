package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

public class ByteSize implements Token {
  private final long bytes;
  private final String original;

  public ByteSize(String value) {
    this.original = value;
    String numPart = value.replaceAll("[^0-9.]", "");
    String unitPart = value.replaceAll("[0-9.]", "").toUpperCase();

    double base = Double.parseDouble(numPart);

    switch (unitPart) {
      case "B":
        this.bytes = (long) base;
        break;
      case "KB":
        this.bytes = (long) (base * 1024);
        break;
      case "MB":
        this.bytes = (long) (base * 1024 * 1024);
        break;
      case "GB":
        this.bytes = (long) (base * 1024 * 1024 * 1024);
        break;
      case "TB":
        this.bytes = (long) (base * 1024L * 1024 * 1024 * 1024);
        break;
      default:
        throw new IllegalArgumentException("Unknown unit in byte size: " + unitPart);
    }
  }

  public long getBytes() {
    return this.bytes;
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
    return TokenType.STRING; // or define a custom TokenType.BYTESIZE
  }
}
