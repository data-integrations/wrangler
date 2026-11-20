package io.cdap.wrangler.api.parser;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.util.Locale;

@PublicEvolving
public class ByteSize implements Token {
  private final long bytes;
  private final String original;

  public ByteSize(String input) {
    this.original = input;
    this.bytes = parseToBytes(input.trim().toUpperCase(Locale.ENGLISH));
  }

  private long parseToBytes(String input) {
    if (input.endsWith("KB")) return parseNumber(input, "KB") * 1024L;
    if (input.endsWith("MB")) return parseNumber(input, "MB") * 1024L * 1024L;
    if (input.endsWith("GB")) return parseNumber(input, "GB") * 1024L * 1024L * 1024L;
    if (input.endsWith("TB")) return parseNumber(input, "TB") * 1024L * 1024L * 1024L * 1024L;
    if (input.endsWith("PB")) return parseNumber(input, "PB") * 1024L * 1024L * 1024L * 1024L * 1024L;
    throw new IllegalArgumentException("Unsupported byte unit: " + input);
  }

  private long parseNumber(String input, String suffix) {
    return (long) Double.parseDouble(input.substring(0, input.length() - suffix.length()));
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
  public JsonObject toJson() {
    JsonObject obj = new JsonObject();
    obj.add("original", new JsonPrimitive(original));
    obj.add("bytes", new JsonPrimitive(bytes));
    return obj;
  }
}
