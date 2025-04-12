/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ByteSize class is used for parsing byte sizes with units such as B, KB, MB, GB, and TB.
 * It converts the size into bytes for consistent handling.
 */
public class ByteSize implements Token {
  // Regular expression to match size with units like B, KB, MB, GB, or TB.
  private static final Pattern PATTERN = Pattern.compile("(?i)([0-9]*\\.?[0-9]+)\\s*([KMGT]?B)");

  private final long bytes;

  /**
   * Constructor that parses the byte size string.
   *
   * @param value the string representing the byte size with unit (e.g., "10KB", "1.5MB")
   */
  public ByteSize(String value) {
    Matcher matcher = PATTERN.matcher(value.trim());
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid byte size format: " + value);
    }

    // Extract the numeric value and the unit (e.g., KB, MB)
    double number = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(2).toUpperCase(Locale.ENGLISH);

    // Convert the value to bytes based on the unit
    switch (unit) {
      case "B":  bytes = (long) number; break;
      case "KB": bytes = (long) (number * 1024); break;
      case "MB": bytes = (long) (number * 1024 * 1024); break;
      case "GB": bytes = (long) (number * 1024 * 1024 * 1024); break;
      case "TB": bytes = (long) (number * 1024L * 1024L * 1024L * 1024L); break;
      default:
        throw new IllegalArgumentException("Unsupported byte unit: " + unit);
    }
  }

  /**
   * Returns the value in bytes.
   *
   * @return the byte size in bytes
   */
  public long getBytes() {
    return bytes;
  }

  @Override
  public String value() {
    return Long.toString(bytes);
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(bytes);
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }
}
