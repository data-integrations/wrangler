package io.cdap.wrangler.api.parser;

import java.util.HashMap;
import java.util.Map;

public class TimeDuration implements Token {
  private final String input;
  private final double value;
  private final String unit;

  private static final Map<String, Long> UNIT_MAP = new HashMap<>();

  static {
    UNIT_MAP.put("NS", 1L);
    UNIT_MAP.put("US", 1000L);
    UNIT_MAP.put("MS", 1000L * 1000);
    UNIT_MAP.put("S", 1000L * 1000 * 1000);
    UNIT_MAP.put("M", 60L * 1000 * 1000 * 1000);
    UNIT_MAP.put("H", 60L * 60 * 1000 * 1000 * 1000);
  }

  public TimeDuration(String input) {
    this.input = input.trim();
    int index = findFirstUnitIndex(this.input);
    this.value = Double.parseDouble(this.input.substring(0, index));
    this.unit = this.input.substring(index).toUpperCase();
  }

  private int findFirstUnitIndex(String input) {
    for (int i = 0; i < input.length(); i++) {
      if (!Character.isDigit(input.charAt(i)) && input.charAt(i) != '.') {
        return i;
      }
    }
    throw new IllegalArgumentException("No unit found in time duration string: " + input);
  }

  public long getNanoseconds() {
    if (!UNIT_MAP.containsKey(unit)) {
      throw new IllegalArgumentException("Unknown time unit: " + unit);
    }
    return (long) (value * UNIT_MAP.get(unit));
  }

  @Override
  public String toString() {
    return input;
  }
}
