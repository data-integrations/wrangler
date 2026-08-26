public class ByteSize extends Token {
  private final long bytes;

  public ByteSize(String value) {
    super(value);
    this.bytes = parse(value);
  }

  private long parse(String value) {
    value = value.trim().toLowerCase();
    double num = Double.parseDouble(value.replaceAll("[a-z]+", ""));
    if (value.endsWith("kb")) return (long)(num * 1024);
    if (value.endsWith("mb")) return (long)(num * 1024 * 1024);
    if (value.endsWith("gb")) return (long)(num * 1024 * 1024 * 1024);
    return (long)num;
  }

  public long getBytes() {
    return bytes;
  }
}
