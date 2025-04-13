package io.cdap.wrangler.api.parser;

public class TimeDuration extends Token {
  private final long milliseconds;

  public TimeDuration(String value) {
    super(value);
    this.milliseconds = parseMilliseconds(value);
  }

  private long parseMilliseconds(String value) {
    value = value.trim().toLowerCase();
    double number = Double.parseDouble(value.replaceAll("[^0-9.]", ""));
    if (value.endsWith("ms")) return (long) number;
    if (value.endsWith("s")) return (long) (number * 1000);
    if (value.endsWith("m")) return (long) (number * 60 * 1000);
    return Long.parseLong(value);
  }

  public long getMilliseconds() {
    return milliseconds;
  }
}