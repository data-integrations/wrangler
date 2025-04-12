package io.cdap.wrangler.api.parser;

public class ByteSize extends Token {
  private final long bytes;

  public ByteSize(String value) {
    super(value);
    this.bytes = parseToBytes(value);
  }

  private long parseToBytes(String input) {
    input = input.trim().toUpperCase();

    double number;
    if (input.endsWith("KB")) {
      number = Double.parseDouble(input.replace("KB", ""));
      return (long) (number * 1024);
    } else if (input.endsWith("MB")) {
      number = Double.parseDouble(input.replace("MB", ""));
      return (long) (number * 1024 * 1024);
    } else if (input.endsWith("GB")) {
      number = Double.parseDouble(input.replace("GB", ""));
      return (long) (number * 1024 * 1024 * 1024);
    } else if (input.endsWith("TB")) {
      number = Double.parseDouble(input.replace("TB", ""));
      return (long) (number * 1024L * 1024L * 1024L * 1024L);
    } else {
      throw new IllegalArgumentException("Unsupported byte unit: " + input);
    }
  }

  public long getBytes() {
    return bytes;
  }
}
