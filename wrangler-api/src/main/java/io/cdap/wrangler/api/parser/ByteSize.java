/*
 * Copyright © 2017-2025 Cask Data, Inc.
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
 * Token implementation for byte size values like "10MB", "2GB", etc.
 */

public class ByteSize implements Token {
  private final String original;
  private final long bytes;

  public ByteSize(String value) {
    this.original = value.trim().toUpperCase();

    if (original.endsWith("KB")) {
      bytes = parseValue(original, "KB", 1024L);
    } else if (original.endsWith("MB")) {
      bytes = parseValue(original, "MB", 1024L * 1024L);
    } else if (original.endsWith("GB")) {
      bytes = parseValue(original, "GB", 1024L * 1024L * 1024L);
    } else if (original.endsWith("B")) {
      bytes = parseValue(original, "B", 1);
    } else {
      throw new IllegalArgumentException("Unsupported byte unit in: " + value);
    }
  }

  private long parseValue(String input, String suffix, long multiplier) {
    double number = Double.parseDouble(input.substring(0, input.length() - suffix.length()));
    return (long) (number * multiplier);
  }

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

  @Override
  public String toString() {
    return original;
  }
}
