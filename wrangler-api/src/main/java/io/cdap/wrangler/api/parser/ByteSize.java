/*
 * Copyright © 2017-2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import java.util.Locale;

/**
 * Represents a byte size value parsed from a string (e.g., "10KB").
 */
public class ByteSize implements Token {
  private final long bytes;

  /**
   * Constructs a {@code ByteSize} by parsing the input string.
   *
   * @param value the string representation (e.g., "10KB", "5MB")
   * @throws IllegalArgumentException if the value is invalid
   */
  public ByteSize(String value) {
    this.bytes = parseByteSize(value);
  }

  /**
   * Parses the input string to calculate the byte size in bytes.
   *
   * @param value the string to parse
   * @return the size in bytes
   * @throws IllegalArgumentException if the format or number is invalid
   */
  private long parseByteSize(String value) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Byte size value cannot be null or empty");
    }
    String val = value.toUpperCase(Locale.ROOT).trim();
    try {
      if (val.endsWith("KB")) {
        return Long.parseLong(val.replace("KB", "").trim()) * 1024L;
      } else if (val.endsWith("MB")) {
        return Long.parseLong(val.replace("MB", "").trim()) * 1024L * 1024L;
      } else if (val.endsWith("GB")) {
        return Long.parseLong(val.replace("GB", "").trim()) * 1024L * 1024L * 1024L;
      } else if (val.endsWith("TB")) {
        return Long.parseLong(val.replace("TB", "").trim()) * 1024L * 1024L * 1024L * 1024L;
      } else if (val.endsWith("PB")) {
        return Long.parseLong(val.replace("PB", "").trim()) * 1024L * 1024L * 1024L * 1024L * 1024L;
      } else if (val.endsWith("B")) {
        return Long.parseLong(val.replace("B", "").trim());
      } else {
        throw new IllegalArgumentException("Invalid byte size format: " + value);
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid number in byte size: " + value, e);
    }
  }

  /**
   * Returns the byte size in bytes.
   *
   * @return the number of bytes
   */
  public long getBytes() {
    return bytes;
  }

  /**
   * Returns the value of the byte size.
   *
   * @return the byte size as a {@code Long}
   */
  @Override
  public Object value() {
    return bytes;
  }

  /**
   * Returns the token type for this byte size.
   *
   * @return the {@code TokenType.BYTE_SIZE}
   */
  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  /**
   * Converts the byte size to a JSON representation.
   *
   * @return a JSON element containing the byte size
   */
  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(bytes);
  }
}
