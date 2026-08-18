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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a byte size token parsed from a directive argument (e.g., "10KB", "1.5MB").
 * Implements the {@link Token} interface and provides a method to retrieve the size in bytes.
 */
public class ByteSize implements Token, Serializable {
  private static final long serialVersionUID = -183587928345928374L;
  private final long bytes;
  private final String originalToken;

  // Pattern to capture the numeric value and the unit (case-insensitive)
  // Allows for optional 'B' after the unit prefix (KB, KiB, MB, MiB, etc.)
  // Embed (?i) for case-insensitivity, use \\. for literal dot
  private static final String BYTE_PATTERN_STRING =
    "(?i)\\s*(\\d+(?:\\.\\d+)?)\\s*([kKmMgGtTpP]?[bB]?)?\\s*";
  private static final Pattern BYTE_PATTERN = Pattern.compile(BYTE_PATTERN_STRING);

  /**
   * Parses the byte size string token.
   * @param token The original string token (e.g., "10KB").
   * @throws DirectiveParseException if the token format is invalid.
   */
  public ByteSize(String token) throws DirectiveParseException {
    this.originalToken = token;

    Matcher matcher = BYTE_PATTERN.matcher(token);
    if (!matcher.matches()) {
      throw new DirectiveParseException("Invalid byte size format: '" + token +
          "'. Expected format like '1024', '10KB', '1.5MB', etc.");
    }

    double numericValue;
    try {
        numericValue = Double.parseDouble(matcher.group(1));
    } catch (NumberFormatException e) {
        throw new DirectiveParseException("Invalid numeric value in byte size token: '" + token + "'", e);
    }

    String unit = matcher.group(2); // Unit prefix (K, M, G, T, P) or null for bytes

    long multiplier = 1L;
    if (unit != null && !unit.isEmpty()) {
      // Handle the case where it's just 'B' or 'b' (bytes)
      if (unit.equalsIgnoreCase("B")) {
        multiplier = 1L; // Already the default, but being explicit
      } else {
        // Extract just the first character for the unit prefix (K, M, G, T, P)
        char prefix = unit.toUpperCase().charAt(0);
        switch (prefix) {
          case 'K':
            multiplier = 1024L;
            break;
          case 'M':
            multiplier = 1024L * 1024L;
            break;
          case 'G':
            multiplier = 1024L * 1024L * 1024L;
            break;
          case 'T':
            multiplier = 1024L * 1024L * 1024L * 1024L;
            break;
          case 'P':
            multiplier = 1024L * 1024L * 1024L * 1024L * 1024L;
            break;
          // Should not happen with the current regex, but good practice
          default:
               throw new DirectiveParseException(
                   "Unknown byte size unit prefix: '" + unit + "' in token '" + token + "'");
        }
      }
    }

    // Handle potential overflow and floating point values
    double exactBytes = numericValue * multiplier;
    if (exactBytes < 0 || exactBytes > Long.MAX_VALUE) { // Also check for negative values
         // Break the message across lines
         String errorMsg = String.format(
           "Byte size value '%s' is out of range (must be non-negative and fit in Long).", token
         );
         throw new DirectiveParseException(errorMsg);
    }
    // We round down for fractional bytes, similar to truncation.
    this.bytes = (long) exactBytes;
  }

  /**
   * @return The size represented by this token in bytes.
   */
  public long getBytes() {
    return bytes;
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
   * @return {@link TokenType#BYTE_SIZE}
   */
  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
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
