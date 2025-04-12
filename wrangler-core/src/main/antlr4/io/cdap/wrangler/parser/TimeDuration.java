package io.cdap.wrangler.api.parser;

public class TimeDuration extends Token {
  private final long milliseconds;

  public TimeDuration(String value) {
    super(value);
    this.milliseconds = parseToMilliseconds(value);
  }

  private long parseToMilliseconds(String input) {
    input = input.trim().toLowerCase();

    double number;
    if (input.endsWith("ms")) {
      number = Double.parseDouble(input.replace("ms", ""));
      return (long) number;
    } else if (input.endsWith("s")) {
      number = Double.parseDouble(input.replace("s", ""));
      return (long) (number * 1000);
    } else if (input.endsWith("min")) {
      number = Double.parseDouble(input.replace("min", ""));
      return (long) (number * 60 * 1000);
    } else if (input.endsWith("h")) {
      number = Double.parseDouble(input.replace("h", ""));
      return (long) (number * 60 * 60 * 1000);
    } else {
      throw new IllegalArgumentException("Unsupported time unit: " + input);
    }
  }

  public long getMilliseconds() {
    return milliseconds;
  }
}
