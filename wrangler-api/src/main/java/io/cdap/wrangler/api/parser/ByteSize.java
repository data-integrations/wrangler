package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ByteSize extends Token {
  private final double value;
  private final String unit;

  public ByteSize(String value) {
    super(Type.BYTE_SIZE, value);
    this.unit = value.replaceAll("[0-9.]", "").toUpperCase();
    this.value = Double.parseDouble(value.replaceAll("[^0-9.]", ""));
  }

  public long getBytes() {
    switch (unit) {
      case "B": return (long) value;
      case "KB": return (long) (value * 1024);
      case "MB": return (long) (value * 1024 * 1024);
      case "GB": return (long) (value * 1024 * 1024 * 1024);
      case "TB": return (long) (value * 1024L * 1024 * 1024 * 1024);
      default: throw new IllegalArgumentException("Unknown byte unit: " + unit);
    }
  }

  @Override
  public Object value() {
    return getBytes();
  }

  @Override
  public JsonElement toJson() {
    JsonObject json = new JsonObject();
    json.addProperty("bytes", getBytes());
    return json;
  }
}