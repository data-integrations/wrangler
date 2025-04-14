package io.cdap.wrangler.api.parser;

public class TimeDuration extends Token {
  private final long nanoseconds;

  public TimeDuration(String value) {
    super(Type.TIME_DURATION, value);
    this.nanoseconds = parseToNanos(value.trim().toLowerCase());
  }

  private long parseToNanos(String value) {
    if (value.endsWith("ns")) {
      return (long) Double.parseDouble(value.replace("ns", ""));
    } else if (value.endsWith("us")) {
      return (long) (Double.parseDouble(value.replace("us", "")) * 1_000);
    } else if (value.endsWith("ms")) {
      return (long) (Double.parseDouble(value.replace("ms", "")) * 1_000_000);
    } else if (value.endsWith("s")) {
      return (long) (Double.parseDouble(value.replace("s", "")) * 1_000_000_000);
    } else if (value.endsWith("m")) {
      return (long) (Double.parseDouble(value.replace("m", "")) * 60L * 1_000_000_000);
    } else if (value.endsWith("h")) {
      return (long) (Double.parseDouble(value.replace("h", "")) * 60L * 60 * 1_000_000_000);
    } else {
      throw new IllegalArgumentException("Unknown time unit: " + value);
    }
  }

  public long getNanoseconds() {
    return nanoseconds;
  }
}
