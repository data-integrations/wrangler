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
import com.google.gson.JsonObject;

/**
 * A token representing a byte size value (e.g., "10mb", "5kb") with its equivalent in bytes.
 */

public class ByteSize implements Token {
  private final String value;
  private final long bytes;

  /**
   * Constructs a {@code ByteSize} token from a string representation.
   *
   * @param value the string representation of the byte size (e.g., "10mb")
   */
  public ByteSize(String value) {
    this.value = value;
    this.bytes = parseBytes(value);
  }

  /**
   * Returns the string value of the byte size.
   *
   * @return the string value
   */
  @Override
  public String value() {
    return value;
  }

  /**
   * Returns the token type as {@code BYTE_SIZE}.
   *
   * @return the token type
   */
  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  /**
   * Converts the byte size to a JSON object containing the value and bytes.
   *
   * @return a {@code JsonElement} representing the token
   */
  @Override
  public JsonElement toJson() {
    JsonObject json = new JsonObject();
    json.addProperty("value", value);
    json.addProperty("bytes", bytes);
    return json;
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
   * Parses a string representation of a byte size into bytes.
   *
   * @param input the string to parse (e.g., "10mb")
   * @return the equivalent number of bytes
   */
  private long parseBytes(String input) {
    String normalized = input.toLowerCase();
    double number = Double.parseDouble(normalized.replaceAll("[a-z]+", ""));
    String unit = normalized.replaceAll("[0-9.]+", "");
    switch (unit) {
      case "kb":
        return (long) (number * 1024);
      case "mb":
        return (long) (number * 1024 * 1024);
      case "gb":
        return (long) (number * 1024 * 1024 * 1024);
      case "tb":
        return (long) (number * 1024 * 1024 * 1024 * 1024);
      case "b":
      default:
        return (long) number;
    }

  }

}
