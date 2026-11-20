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
import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * The ByteSize class represents a token that includes a numeric value and a byte unit (KB, MB, etc.).
 * It provides functionality to parse byte size strings and convert them to canonical bytes.
 */
@PublicEvolving
public class ByteSize implements Token {
  private final double number;
  private final String unit;
  private final String value;

  public ByteSize(String value) {
    this.value = value;
    String numberPart = value.replaceAll("[^0-9.]", "");
    String unitPart = value.replaceAll("[0-9.]", "");

    this.number = Double.parseDouble(numberPart);
    this.unit = unitPart;
  }

  /**
   * Returns the canonical size in bytes.
   *
   * @return long value representing the size in bytes
   */
  public long getBytes() {
    switch (unit.toUpperCase()) {
      case "B":
        return (long) number;
      case "KB":
        return (long) (number * 1024);
      case "MB":
        return (long) (number * 1024 * 1024);
      case "GB":
        return (long) (number * 1024 * 1024 * 1024);
      case "TB":
        return (long) (number * 1024 * 1024 * 1024 * 1024);
      case "PB":
        return (long) (number * 1024 * 1024 * 1024 * 1024 * 1024);
      default:
        throw new IllegalArgumentException("Invalid byte unit: " + unit);
    }
  }

  @Override
  public Object value() {
    return value;
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", type().name());
    object.addProperty("value", value);
    return object;
  }
}