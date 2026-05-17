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
 * This class {@code ByteSize} represents a byte size specification token.
 * It supports formats like "1B", "1KB", "1MB", "1GB", or "1TB".
 */
@PublicEvolving
public class ByteSize implements Token, Serializable {
  private static final long serialVersionUID = 1L;
  private final String value;
  private static final Pattern BYTE_SIZE_PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)(B|KB|MB|GB|TB)");

  public ByteSize(String value) {
    this.value = value;
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  @Override
  public Object value() {
    return value;
  }

  @Override
  public String toString() {
    return value;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", type().name());
    object.addProperty("value", value);
    return object;
  }

  public long getBytes() {
    Matcher matcher = BYTE_SIZE_PATTERN.matcher(value.toUpperCase());
    if (!matcher.matches()) {
      return 0L;
    }
    double size = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(2);
    switch (unit.toUpperCase()) {
      case "B":
        return Math.round(size);
      case "KB":
        return Math.round(size * 1024);
      case "MB":
        return Math.round(size * 1024 * 1024);
      case "GB":
        return Math.round(size * 1024 * 1024 * 1024);
      case "TB":
        return Math.round(size * 1024 * 1024 * 1024 * 1024);
      default:
        return 0L;
    }
  }

  public String convertTo(String targetUnit) {
    long bytes = getBytes();
    if (bytes == 0) {
      return "0" + targetUnit; // Return zero with target unit
    }

    double convertedValue;
    switch (targetUnit.toUpperCase()) {
      case "B":
        convertedValue = bytes;
        break;
      case "KB":
        convertedValue = bytes / 1024.0;
        break;
      case "MB":
        convertedValue = bytes / (1024.0 * 1024);
        break;
      case "GB":
        convertedValue = bytes / (1024.0 * 1024 * 1024);
        break;
      case "TB":
        convertedValue = bytes / (1024.0 * 1024 * 1024 * 1024);
        break;
      default:
        return value; // Return original value if invalid unit
    }

    // Format with at most one decimal place, removing trailing zeros
    String formatted = String.format("%.1f", convertedValue).replaceAll("\\.?0*$", "");
    return formatted + targetUnit;
  }
} 
