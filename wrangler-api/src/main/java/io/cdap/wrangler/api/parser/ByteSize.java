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
 * Token implementation for ByteSize like "10KB", "5MB", etc.
 */
@PublicEvolving
public class ByteSize implements Token {
  private final long bytes;
  private final String original;

  public ByteSize(String value) {
    this.original = value.trim().toUpperCase();
    System.out.println("Parsing size: " + original);
    this.bytes = parseBytes(original);
  }

  private long parseBytes(String value) {
    if (value.endsWith("B") && !value.endsWith("KB") && !value.endsWith("MB") &&
        !value.endsWith("GB") && !value.endsWith("TB") && !value.endsWith("PB")) {
      return Long.parseLong(value.substring(0, value.length() - 1));
    } else if (value.endsWith("KB")) {
      return (long) (Double.parseDouble(value.replace("KB", "")) * 1024);
    } else if (value.endsWith("MB")) {
      return (long) (Double.parseDouble(value.replace("MB", "")) * 1024 * 1024);
    } else if (value.endsWith("GB")) {
      return (long) (Double.parseDouble(value.replace("GB", "")) * 1024 * 1024 * 1024);
    } else if (value.endsWith("TB")) {
      return (long) (Double.parseDouble(value.replace("TB", "")) * 1024L * 1024 * 1024 * 1024);
    } else if (value.endsWith("PB")) {
      return (long) (Double.parseDouble(value.replace("PB", "")) * 1024L * 1024 * 1024 * 1024 * 1024);
    } else {
      throw new IllegalArgumentException("Invalid byte size format: " + value);
    }
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
    JsonObject json = new JsonObject();
    json.addProperty("type", TokenType.BYTE_SIZE.name());
    json.addProperty("value", bytes);
    return json;
  }
}
