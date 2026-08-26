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

/**
 * Represents a byte size value with methods to parse and retrieve the size in bytes.
 */
public class ByteSize implements Token {
  private final String original;
  private final long bytes;

  /**
   * Constructs a ByteSize object from the provided value.
   *
   * @param value the byte size value as a string (e.g., "10MB", "500KB")
   */
  public ByteSize(String value) {
    this.original = value;
    this.bytes = parse(value);
  }

  /**
   * Parses the provided byte size string and converts it into bytes.
   *
   * @param value the byte size string (e.g., "10MB", "500KB")
   * @return the size in bytes
   * @throws IllegalArgumentException if the byte size is invalid
   */
  private long parse(String value) {
    value = value.trim().toLowerCase();
    double number = Double.parseDouble(value.replaceAll("[^0-9.]", ""));
    
    if (value.endsWith("kb")) {
      return (long) (number * 1024);
    } 
    if (value.endsWith("mb")) {
      return (long) (number * 1024 * 1024);
    }
    if (value.endsWith("gb")) {
      return (long) (number * 1024 * 1024 * 1024);
    }
    if (value.endsWith("tb")) {
      return (long) (number * 1024L * 1024 * 1024 * 1024);
    }
    if (value.endsWith("b")) {
      return (long) number;
    }

    throw new IllegalArgumentException("Invalid byte size: " + value);
  }

  /**
   * Returns the byte size in bytes.
   *
   * @return the size in bytes
   */
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
