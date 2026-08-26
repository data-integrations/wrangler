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
import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * A token representing a byte size value (e.g., "10KB", "1.5MB").
 */
@PublicEvolving
public class ByteSize implements Token {
  private final String value;
  private final long bytes;

  public ByteSize(String value) {
    this.value = value;
    this.bytes = parseByteSize(value);
  }

  private long parseByteSize(String input) {
    String num = input.replaceAll("[^0-9.]", "");
    String unit = input.replaceAll("[0-9.]", "").toUpperCase();
    double number = Double.parseDouble(num);
    switch (unit) {
      case "KB":
        return (long) (number * 1024);
      case "MB":
        return (long) (number * 1024 * 1024);
      case "GB":
        return (long) (number * 1024 * 1024 * 1024);
      case "TB":
        return (long) (number * 1024 * 1024 * 1024 * 1024);
      default:
        return (long) number; // Bytes
    }
  }

  public long getBytes() {
    return bytes;
  }

  @Override
  public String value() {
    return value;
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(value);
  }
}
