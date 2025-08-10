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
 * The ByteSize class wraps byte size values with units (KB, MB, GB, etc.)
 * and provides methods to convert between different units.
 */
@PublicEvolving
public class ByteSize implements Token {
  private final long bytes;
  private final String originalValue;

  public ByteSize(String value) {
    this.originalValue = value;
    this.bytes = parseBytes(value);
  }

  private long parseBytes(String value) {
    value = value.trim().toUpperCase();
    long multiplier = 1;
    String number = value;

    if (value.endsWith("KB")) {
      multiplier = 1024;
      number = value.substring(0, value.length() - 2);
    } else if (value.endsWith("MB")) {
      multiplier = 1024 * 1024;
      number = value.substring(0, value.length() - 2);
    } else if (value.endsWith("GB")) {
      multiplier = 1024 * 1024 * 1024;
      number = value.substring(0, value.length() - 2);
    } else if (value.endsWith("B")) {
      number = value.substring(0, value.length() - 1);
    }

    try {
      double val = Double.parseDouble(number);
      return (long) (val * multiplier);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid byte size format: " + value);
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
    object.addProperty("value", originalValue);
    object.addProperty("bytes", bytes);
    return object;
  }

  public long getBytes() {
    return bytes;
  }

  public double getKB() {
    return bytes / 1024.0;
  }

  public double getMB() {
    return bytes / (1024.0 * 1024.0);
  }

  public double getGB() {
    return bytes / (1024.0 * 1024.0 * 1024.0);
  }
} 