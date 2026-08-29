package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a time duration token parsed from a string (e.g., "150ms", "2.1s").
 * The value is stored in canonical units (milliseconds).
 */
public class TimeDuration implements Token {
  // The original token string.
  private final String token;
  // Canonical value stored in milliseconds.
  private final long milliseconds;
  // Token type.
  private final TokenType tokenType = TokenType.TIME_DURATION;

  // Constant: 1 second = 1000 milliseconds.
  private static final long SECOND_IN_MS = 1000;

  // Pattern to capture the numeric and unit portions (e.g., "150ms" or "2.1s").
  private static final Pattern PATTERN = Pattern.compile("([0-9]+(?:\\.[0-9]+)?)([a-zA-Z]+)");

  /**
   * Constructs a TimeDuration token by parsing the input string.
   *
   * @param token the string representation (e.g., "150ms", "2.1s")
   * @throws IllegalArgumentException if the token is null, empty, or its format is invalid.
   */
  public TimeDuration(String token) {
    if (token == null || token.trim().isEmpty()) {
      throw new IllegalArgumentException("Token cannot be null or empty");
    }
    this.token = token;
    Matcher matcher = PATTERN.matcher(token);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid time duration token format: " + token);
    }

    String numberStr = matcher.group(1);
    // Normalize the unit to lower-case for easier comparison.
    String unitStr = matcher.group(2).toLowerCase();

    double value = Double.parseDouble(numberStr);
    long multiplier;

    // Determine multiplier based on the unit.
    if ("ms".equals(unitStr)) {
      multiplier = 1;
    } else if ("s".equals(unitStr)) {
      multiplier = SECOND_IN_MS;
    } else {
      throw new IllegalArgumentException("Unsupported time duration unit: " + unitStr);
    }

    // Calculate canonical value in milliseconds.
    this.milliseconds = (long) Math.round(value * multiplier);
  }

  /**
   * Returns the time duration in canonical units (milliseconds).
   *
   * @return the duration in milliseconds.
   */
  public long getMilliseconds() {
    return milliseconds;
  }

  @Override
  public Object value() {
    // Return the canonical value.
    return milliseconds;
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
    obj.addProperty("milliseconds", milliseconds);
    return obj;
  }
}
