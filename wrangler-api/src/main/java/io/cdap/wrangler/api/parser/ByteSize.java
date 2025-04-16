package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a byte size token parsed from a string (e.g., "10KB", "1.5MB").
 * The value is stored in canonical units (bytes).
 */
public class ByteSize implements Token {
  // The original token string.
  private final String token;
  // Canonical value stored in bytes.
  private final long bytes;
  // Token type.
  private final TokenType tokenType = TokenType.BYTE_SIZE;

  // Conversion constants (using 1024-based conversions)
  private static final long KILOBYTE = 1024;
  private static final long MEGABYTE = KILOBYTE * 1024;
  private static final long GIGABYTE = MEGABYTE * 1024;
  private static final long TERABYTE = GIGABYTE * 1024;

  // Pattern to capture the numeric and unit portions (e.g., "10KB" or "1.5MB").
  private static final Pattern PATTERN = Pattern.compile("([0-9]+(?:\\.[0-9]+)?)([a-zA-Z]+)");

  /**
   * Constructs a ByteSize token by parsing the input string.
   *
   * @param token the string representation (e.g., "10KB", "1.5MB")
   * @throws IllegalArgumentException if the token is null, empty, or its format is invalid.
   */
  public ByteSize(String token) {
    if (token == null || token.trim().isEmpty()) {
      throw new IllegalArgumentException("Token cannot be null or empty");
    }
    this.token = token;
    Matcher matcher = PATTERN.matcher(token);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid byte size token format: " + token);
    }

    String numberStr = matcher.group(1);
    String unitStr = matcher.group(2).toUpperCase(); // Normalize unit to upper-case

    double value = Double.parseDouble(numberStr);
    long multiplier;

    // Determine multiplier based on the unit.
    if ("B".equals(unitStr)) {
      multiplier = 1;
    } else if ("KB".equals(unitStr)) {
      multiplier = KILOBYTE;
    } else if ("MB".equals(unitStr)) {
      multiplier = MEGABYTE;
    } else if ("GB".equals(unitStr)) {
      multiplier = GIGABYTE;
    } else if ("TB".equals(unitStr)) {
      multiplier = TERABYTE;
    } else {
      throw new IllegalArgumentException("Unsupported byte size unit: " + unitStr);
    }

    // Calculate canonical value in bytes.
    this.bytes = (long) Math.round(value * multiplier);
  }

  /**
   * Returns the byte size in canonical units (bytes).
   *
   * @return the size in bytes.
   */
  public long getBytes() {
    return bytes;
  }

  @Override
  public Object value() {
    // Return the canonical value.
    return bytes;
  }

  @Override
  public TokenType type() {
    return tokenType;
  }

  @Override
  public JsonElement toJson() {
    // Construct a JSON representation of this token.
    JsonObject obj = new JsonObject();
    obj.addProperty("token", token);
    obj.addProperty("type", tokenType.name());
    obj.addProperty("bytes", bytes);
    return obj;
  }
}
