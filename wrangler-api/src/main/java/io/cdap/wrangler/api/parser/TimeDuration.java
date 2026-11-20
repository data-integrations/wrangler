/*
 * Copyright © 2025 CDAP
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import io.cdap.wrangler.api.DirectiveParseException;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a time duration token parsed from a directive argument (e.g., "150ms", "2.5s", "1m").
 * Implements the {@link Token} interface and provides a method to retrieve the duration in nanoseconds.
 */
public class TimeDuration implements Token, Serializable {
  private static final long serialVersionUID = -892374592387459283L;
  private final long nanoseconds;
  private final String originalToken;

  // Pattern to capture the numeric value and the unit (case-insensitive)
  // Units: ns, us (or µs), ms, s, m, h, d
  // Embed (?i) for case-insensitivity, use \\. for literal dot
  private static final String TIME_PATTERN_STRING =
    "(?i)\\s*(\\d+(?:\\.\\d+)?)\\s*([nN][sS]|[uUµ][sS]|[mM][sS]|[sS]|[mM]|[hH]|[dD])?\\s*";
  private static final Pattern TIME_PATTERN = Pattern.compile(TIME_PATTERN_STRING);

  private static final long NANOS_PER_MICRO = 1000L;
  private static final long NANOS_PER_MILLI = NANOS_PER_MICRO * 1000L;
  private static final long NANOS_PER_SECOND = NANOS_PER_MILLI * 1000L;
  private static final long NANOS_PER_MINUTE = NANOS_PER_SECOND * 60L;
  private static final long NANOS_PER_HOUR = NANOS_PER_MINUTE * 60L;
  private static final long NANOS_PER_DAY = NANOS_PER_HOUR * 24L;

  /**
   * Parses the time duration string token.
   * @param token The original string token (e.g., "150ms").
   * @throws DirectiveParseException if the token format is invalid.
   */
  public TimeDuration(String token) throws DirectiveParseException {
    this.originalToken = token;

    Matcher matcher = TIME_PATTERN.matcher(token);
    if (!matcher.matches()) {
      throw new DirectiveParseException("Invalid time duration format: '" + token +
          "'. Expected format like '100ns', '50us', '150ms', '2.5s', '1m', '2h', '3d'.");
    }

    double numericValue;
    try {
        numericValue = Double.parseDouble(matcher.group(1));
    } catch (NumberFormatException e) {
        throw new DirectiveParseException("Invalid numeric value in time duration token: '" + token + "'", e);
    }

    String unit = matcher.group(2); // Unit (ns, us, ms, s, m, h, d) or null (implies seconds)

    long multiplierNanos = NANOS_PER_SECOND; // Default unit is seconds if not specified

    if (unit != null) {
      switch (unit.toLowerCase()) {
        case "ns":
          multiplierNanos = 1L;
          break;
        case "us":
        case "µs": // Support microsecond symbol
          multiplierNanos = NANOS_PER_MICRO;
          break;
        case "ms":
          multiplierNanos = NANOS_PER_MILLI;
          break;
        case "s":
          multiplierNanos = NANOS_PER_SECOND;
          break;
        case "m":
          multiplierNanos = NANOS_PER_MINUTE;
          break;
        case "h":
          multiplierNanos = NANOS_PER_HOUR;
          break;
        case "d":
          multiplierNanos = NANOS_PER_DAY;
          break;
        // Should not happen with the current regex, but good practice
        default:
             throw new DirectiveParseException("Unknown time duration unit: '" + unit + "' in token '" + token + "'");
      }
    }

    // Handle potential overflow and floating point values
    double exactNanos = numericValue * multiplierNanos;
    if (exactNanos < 0 || exactNanos > Long.MAX_VALUE) { // Also check for negative values
         // Break the message across lines
         String errorMsg = String.format(
           "Time duration '%s' is out of range (must be non-negative and fit Long nanoseconds).", token
         );
         throw new DirectiveParseException(errorMsg);
    }

    // Round down for fractional nanoseconds
    this.nanoseconds = (long) exactNanos;
  }

  /**
   * @return The duration represented by this token in nanoseconds.
   */
  public long getNanoseconds() {
    return nanoseconds;
  }

  /**
   * Returns the original token string.
   * @return The original string value of the token.
   */
  @Override
  public Object value() {
    return originalToken;
  }

  /**
   * Returns the type of Token.
   * @return {@link TokenType#TIME_DURATION}
   */
  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  /**
   * Converts the token into {@link JsonElement}.
   * @return {@link JsonPrimitive} containing the original token string.
   */
  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(originalToken);
  }
}
