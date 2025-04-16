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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * A {@link Token} for representing byte sizes.
 */
@PublicEvolving
public class ByteSize implements Token {
  private static final Pattern BYTE_SIZE_PATTERN = Pattern.compile("(\\d+)([BKMGTP]B?)");
  private final long bytes;

  public ByteSize(String value) {
    Matcher matcher = BYTE_SIZE_PATTERN.matcher(value);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid byte size format: " + value);
    }

    long size = Long.parseLong(matcher.group(1));
    String unit = matcher.group(2).toUpperCase();

    switch (unit) {
      case "B":
        bytes = size;
        break;
      case "KB":
        bytes = size * 1024;
        break;
      case "MB":
        bytes = size * 1024 * 1024;
        break;
      case "GB":
        bytes = size * 1024 * 1024 * 1024;
        break;
      case "TB":
        bytes = size * 1024L * 1024 * 1024 * 1024;
        break;
      case "PB":
        bytes = size * 1024L * 1024 * 1024 * 1024 * 1024;
        break;
      default:
        throw new IllegalArgumentException("Invalid byte size unit: " + unit);
    }
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", "byte_size");
    object.addProperty("value", bytes);
    return object;
  }

  @Override
  public Object value() {
    return bytes;
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }
} 