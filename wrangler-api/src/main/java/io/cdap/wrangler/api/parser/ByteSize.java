/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.annotations.PublicEvolving;

@PublicEvolving
public class ByteSize implements Token {

  private final long bytes;

  public ByteSize(String value) {
    if (value == null) {
      throw new IllegalArgumentException("Byte size string cannot be null.");
    }
    this.bytes = parseByteSize(value);
  }

  private long parseByteSize(String value) {
    value = value.trim();
    if (value.isEmpty()) {
      throw new IllegalArgumentException("Byte size string cannot be empty.");
    }

    String numberPart = value.replaceAll("[^0-9.\\-]", "");
    String unitPart = value.replaceAll("[0-9.\\-]", "").toUpperCase();

    if (numberPart.isEmpty() || unitPart.isEmpty()) {
      throw new IllegalArgumentException("Invalid byte size format: " + value);
    }

    double numericValue = Double.parseDouble(numberPart);

    if (numericValue < 0) {
      throw new IllegalArgumentException("Negative byte size is not allowed.");
    }

    switch (unitPart) {
      case "B":
        return (long) numericValue;
      case "KB":
        return (long) (numericValue * 1024L);
      case "MB":
        return (long) (numericValue * 1024L * 1024L);
      case "GB":
        return (long) (numericValue * 1024L * 1024L * 1024L);
      case "TB":
        return (long) (numericValue * 1024L * 1024L * 1024L * 1024L);
      default:
        throw new IllegalArgumentException("Unknown Byte unit: " + unitPart);
    }
  }

  public static double convert(long bytes, String unit) {
    switch (unit.toUpperCase()) {
      case "B":
        return bytes;
      case "KB":
        return bytes / 1024.0;
      case "MB":
        return bytes / (1024.0 * 1024);
      case "GB":
        return bytes / (1024.0 * 1024 * 1024);
      case "TB":
        return bytes / (1024.0 * 1024 * 1024 * 1024);
      default:
        throw new IllegalArgumentException("Unknown Byte unit: " + unit);
    }
  }

  @Override
  public Long value() {
    return bytes;
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.BYTE_SIZE.name());
    object.addProperty("value", bytes);
    return object;
  }
}
