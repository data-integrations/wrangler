package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses duration values such as "10s", "5min", "2h", "1d" into nanoseconds.
 */
public class TimeDuration implements Token {
  private static final Pattern TIME_PATTERN = Pattern.compile("^([0-9]*\\.?[0-9]+)\\s*([a-zA-Z]+)$", Pattern.CASE_INSENSITIVE);
  private static final Map<String, Long> TIME_MULTIPLIERS = Map.ofEntries(
    Map.entry("ns", 1L), Map.entry("nanoseconds", 1L),
    Map.entry("us", 1_000L), Map.entry("microseconds", 1_000L),
    Map.entry("ms", 1_000_000L), Map.entry("milliseconds", 1_000_000L),
    Map.entry("s", 1_000_000_000L), Map.entry("sec", 1_000_000_000L), Map.entry("secs", 1_000_000_000L), Map.entry("seconds", 1_000_000_000L),
    Map.entry("m", 60_000_000_000L), Map.entry("min", 60_000_000_000L), Map.entry("mins", 60_000_000_000L), Map.entry("minutes", 60_000_000_000L),
    Map.entry("h", 3_600_000_000_000L), Map.entry("hr", 3_600_000_000_000L), Map.entry("hrs", 3_600_000_000_000L), Map.entry("hours", 3_600_000_000_000L)
  );

  private final String rawValue;
  private final long nanoseconds;

  public TimeDuration(String value) {
    this.rawValue = value.trim();
    this.nanoseconds = parseToNanoseconds(this.rawValue);
  }

  private long parseToNanoseconds(String input) {
    Matcher matcher = TIME_PATTERN.matcher(input.trim().toLowerCase());
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid time duration format: " + input);
    }

    double number = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(2).toLowerCase();

    Long multiplier = TIME_MULTIPLIERS.get(unit);
    if (multiplier == null) {
      throw new IllegalArgumentException("Unsupported time unit: " + unit);
    }

    return (long) (number * multiplier);
  }

  public long getNanoseconds() {
    return nanoseconds;
  }

  @Override
  public Object value() {
    return nanoseconds;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(nanoseconds);
  }
}
