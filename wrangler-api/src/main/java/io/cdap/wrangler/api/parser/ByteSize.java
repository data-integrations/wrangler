/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
// --- File: ByteSize.java ---
package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

/**
 * Token representing a byte size value (e.g., "10KB", "2.5MB").
 */
public class ByteSize implements Token {
  private final long bytes;

  public ByteSize(String value) {
    String unit = value.replaceAll("[0-9.]", "").toUpperCase();
    double number = Double.parseDouble(value.replaceAll("[^0-9.]", ""));
    switch (unit) {
      case "KB": this.bytes = (long) (number * 1024); break;
      case "MB": this.bytes = (long) (number * 1024 * 1024); break;
      case "GB": this.bytes = (long) (number * 1024 * 1024 * 1024); break;
      default: this.bytes = (long) number; break;
    }
  }

  public long getBytes() {
    return this.bytes;
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

