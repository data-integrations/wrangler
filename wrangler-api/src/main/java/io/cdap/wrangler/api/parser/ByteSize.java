/*
 * Copyright © 2025 Cask Data, Inc.
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

public class ByteSize implements Token {
  private final double valueInBytes;
  private final String rawValue;

  public ByteSize(String value) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Byte size value cannot be null or empty");
    }
    this.rawValue = value.trim();
    this.valueInBytes = parse(this.rawValue);
  }

  private double parse(String value) {
    value = value.trim().toUpperCase();

    if (value.endsWith("KB")) {
      return Double.parseDouble(value.replace("KB", "")) * 1024;
    } else if (value.endsWith("MB")) {
      return Double.parseDouble(value.replace("MB", "")) * 1024 * 1024;
    } else if (value.endsWith("GB")) {
      return Double.parseDouble(value.replace("GB", "")) * 1024 * 1024 * 1024;
    } else if (value.endsWith("TB")) {
      return Double.parseDouble(value.replace("TB", "")) * 1024 * 1024 * 1024 * 1024L;
    }

    throw new IllegalArgumentException("Unsupported byte size unit in value: " + value);
  }

  public double getBytes() {
    return valueInBytes;
  }

  @Override
  public Object value() {
    return valueInBytes;
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(valueInBytes);
  }
}