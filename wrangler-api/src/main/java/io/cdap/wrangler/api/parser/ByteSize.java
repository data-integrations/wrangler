package io.cdap.wrangler.api.parser;

public class ByteSize extends Token {
  private final long bytes;

  public ByteSize(String value) {
    super(value);
    this.bytes = parseBytes(value);
  }

  private long parseBytes(String value) {
    value = value.trim().toUpperCase();
    double number = Double.parseDouble(value.replaceAll("[^0-9.]", ""));
    if (value.endsWith("KB")) return (long) (number * 1024);
    if (value.endsWith("MB")) return (long) (number * 1024 * 1024);
    if (value.endsWith("GB")) return (long) (number * 1024 * 1024 * 1024);
    return Long.parseLong(value);
  }

  public long getBytes() {
    return bytes;
  }
}