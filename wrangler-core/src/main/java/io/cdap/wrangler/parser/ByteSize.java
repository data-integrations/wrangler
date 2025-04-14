package io.cdap.wrangler.api.parser;

public class ByteSize extends Token {
  private final long bytes;

  public ByteSize(String value) {
    super(Type.BYTE_SIZE, value);
    this.bytes = parseToBytes(value.trim().toUpperCase());
  }

  private long parseToBytes(String value) {
    if (value.endsWith("KB")) {
      return (long) (Double.parseDouble(value.replace("KB", "")) * 1024);
    } else if (value.endsWith("MB")) {
      return (long) (Double.parseDouble(value.replace("MB", "")) * 1024 * 1024);
    } else if (value.endsWith("GB")) {
      return (long) (Double.parseDouble(value.replace("GB", "")) * 1024 * 1024 * 1024);
    } else if (value.endsWith("TB")) {
      return (long) (Double.parseDouble(value.replace("TB", "")) * 1024L * 1024 * 1024 * 1024);
    } else if (value.endsWith("B")) {
      return (long) Double.parseDouble(value.replace("B", ""));
    } else {
      throw new IllegalArgumentException("Unknown byte unit: " + value);
    }
  }

  public long getBytes() {
    return bytes;
  }
}
