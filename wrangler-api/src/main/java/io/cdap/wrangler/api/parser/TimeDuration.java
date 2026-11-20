public class TimeDuration extends Token {
  private final long millis;

  public TimeDuration(String value) {
    super(value);
    this.millis = parse(value);
  }

  private long parse(String value) {
    value = value.trim().toLowerCase();
    double num = Double.parseDouble(value.replaceAll("[a-z]+", ""));
    if (value.endsWith("ms")) return (long)num;
    if (value.endsWith("s")) return (long)(num * 1000);
    if (value.endsWith("m")) return (long)(num * 60000);
    return (long)num;
  }

  public long getMillis() {
    return millis;
  }
}
