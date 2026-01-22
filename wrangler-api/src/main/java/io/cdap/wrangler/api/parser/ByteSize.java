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

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A {@link Token} type for representing byte size values with units (e.g., 10KB, 1.5MB).
 */
@PublicEvolving
public class ByteSize implements Token {
  private static final Pattern BYTE_SIZE_PATTERN = 
      Pattern.compile("^(\\d+(?:\\.\\d+)?)\\s*([KMGTP]?B)$", Pattern.CASE_INSENSITIVE);
  
  private final String original;
  private final long bytes;

  public ByteSize(String value) {
    this.original = value;
    Matcher matcher = BYTE_SIZE_PATTERN.matcher(value.trim());
    if (!matcher.matches()) {
      throw new IllegalArgumentException(String.format(
          "Invalid byte size format '%s'. Expected format is <number><unit> where unit is B, KB, MB, GB, or TB", 
          value));
    }

    double size = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(2).toUpperCase();

    switch (unit) {
      case "B":
        bytes = (long) size;
        break;
      case "KB":
        bytes = (long) (size * 1024);
        break;
      case "MB":
        bytes = (long) (size * 1024 * 1024);
        break;
      case "GB":
        bytes = (long) (size * 1024 * 1024 * 1024);
        break;
      case "TB":
        bytes = (long) (size * 1024 * 1024 * 1024 * 1024);
        break;
      default:
        throw new IllegalArgumentException("Unsupported byte size unit: " + unit);
    }
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
    JsonObject object = new JsonObject();
    object.addProperty("type", type().name());
    object.addProperty("value", original);
    object.addProperty("bytes", bytes);
    return object;
  }

  /**
   * @return the byte size in bytes
   */
  public long getBytes() {
    return bytes;
  }

  /**
   * @return the original string representation
   */
  public String getOriginal() {
    return original;
  }
}
