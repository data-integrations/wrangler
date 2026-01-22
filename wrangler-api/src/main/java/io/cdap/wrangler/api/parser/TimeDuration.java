package io.cdap.wrangler.api.parser;

import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@PublicEvolving
public class TimeDuration extends Token {
  private static final Pattern PATTERN = Pattern.compile("^(\\d+\\.?\\d*)([A-Za-z]+)$");
  private final long nanoseconds;

  public TimeDuration(String value) {
    super(TokenType.TIME_DURATION, value);
    
    Matcher matcher = PATTERN.matcher(value);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid time duration format: " + value);
    }
    
    double number = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(2).toUpperCase();
    
    switch (unit) {
      case "NS": nanoseconds = (long) number; break;
      case "US": nanoseconds = (long) (number * 1000); break;
      case "MS": nanoseconds = (long) (number * 1000 * 1000); break;
      case "S": nanoseconds = (long) (number * 1000 * 1000 * 1000); break;
      case "M": nanoseconds = (long) (number * 60L * 1000 * 1000 * 1000); break;
      case "H": nanoseconds = (long) (number * 60L * 60 * 1000 * 1000 * 1000); break;
      case "D": nanoseconds = (long) (number * 24L * 60 * 60 * 1000 * 1000 * 1000); break;
      default: throw new IllegalArgumentException("Unknown time unit: " + unit);
    }
  }

  public long getNanoseconds() {
    return nanoseconds;
  }
  
  public double getDurationAs(String unit) {
    unit = unit.toUpperCase();
    switch (unit) {
      case "NS": return nanoseconds;
      case "US": return nanoseconds / 1000.0;
      case "MS": return nanoseconds / (1000.0 * 1000);
      case "S": return nanoseconds / (1000.0 * 1000 * 1000);
      case "M": return nanoseconds / (60.0 * 1000 * 1000 * 1000);
      case "H": return nanoseconds / (60.0 * 60 * 1000 * 1000 * 1000);
      case "D": return nanoseconds / (24.0 * 60 * 60 * 1000 * 1000 * 1000);
      default: throw new IllegalArgumentException("Unknown time unit: " + unit);
    }
  }
}