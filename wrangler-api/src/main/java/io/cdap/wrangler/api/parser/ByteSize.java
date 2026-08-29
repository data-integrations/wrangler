package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ByteSize implements Token {
  private static final Pattern BYTE_PATTERN = Pattern.compile("^([0-9]*\\.?[0-9]+)\\s*(B|KB|MB|GB|TB|PB)$", Pattern.CASE_INSENSITIVE);
  private static final Map<String, Long> MULTIPLIERS = Map.of(
    "B", 1L,
    "KB", 1024L,
    "MB", 1024L * 1024,
    "GB", 1024L * 1024 * 1024,
    "TB", 1024L * 1024 * 1024 * 1024,
    "PB", 1024L * 1024 * 1024 * 1024 * 1024
  );

  private final String rawValue;
  private final long bytes;

  public ByteSize(String value) {
    this.rawValue = value.trim();
    this.bytes = parseToBytes(this.rawValue);
  }

  private long parseToBytes(String input) {
    Matcher matcher = BYTE_PATTERN.matcher(input.trim().toUpperCase());
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid byte size format: " + input);
    }

    double number = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(2).toUpperCase();

    Long multiplier = MULTIPLIERS.get(unit);
    if (multiplier == null) {
      throw new IllegalArgumentException("Unsupported byte unit: " + unit);
    }

    return (long) (number * multiplier);
  }

  public long getBytes() {
    return bytes;
  }

  @Override
  public Object value() {
    return bytes;
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(bytes);
  }
}
