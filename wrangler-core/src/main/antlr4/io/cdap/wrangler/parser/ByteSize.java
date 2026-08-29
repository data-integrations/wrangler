package io.cdap.wrangler.api.parser;

import java.util.HashMap;
import java.util.Map;

public class ByteSize implements Token {
  private final String input;
  private final double value;
  private final String unit;

  private static final Map<String, Long> UNIT_MAP = new HashMap<>();

  static {
    UNIT_MAP.put("B", 1L);
    UNIT_MAP.put("KB", 1024L);
    UNIT_MAP.put("MB", 1024L * 1024);
    UNIT_MAP.put("GB", 1024L * 1024 * 1024);
    UNIT_MAP.put("TB", 1024L * 1024 * 1024 * 1024);
    UNIT_MAP.put("KiB", 1024L);
    UNIT_MAP.put("MiB", 1024L * 1024);
    UNIT_MAP.put("GiB", 1024L * 1024 * 1024);
  }

  public ByteSize(String input) {
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
    throw new IllegalArgumentException("No unit found in byte size string: " + input);
  }

  public long getBytes() {
    if (!UNIT_MAP.containsKey(unit)) {
      throw new IllegalArgumentException("Unknown byte unit: " + unit);
    }
    return (long) (value * UNIT_MAP.get(unit));
  }

  @Override
  public String toString() {
    return input;
  }
}
