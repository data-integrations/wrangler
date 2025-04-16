/*
 * Copyright [year] [your name or organization]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Token class to parse and represent byte size values like "10KB", "5MB", etc.
 */
public class ByteSize implements Token {
  private static final Map<String, Long> UNIT_MULTIPLIERS = new HashMap<>();
  private static final Pattern PATTERN = Pattern.compile("(?i)(\\d+(\\.\\d+)?)([KMGTPE]?B)");

  static {
    UNIT_MULTIPLIERS.put("B", 1L);
    UNIT_MULTIPLIERS.put("KB", 1024L);
    UNIT_MULTIPLIERS.put("MB", 1024L * 1024);
    UNIT_MULTIPLIERS.put("GB", 1024L * 1024 * 1024);
    UNIT_MULTIPLIERS.put("TB", 1024L * 1024 * 1024 * 1024);
    UNIT_MULTIPLIERS.put("PB", 1024L * 1024 * 1024 * 1024 * 1024);
    UNIT_MULTIPLIERS.put("EB", 1024L * 1024 * 1024 * 1024 * 1024 * 1024);
  }

  private final long bytes;
  private final String original;

  public ByteSize(String value) {
    this.original = value;
    this.bytes = parse(value);
  }

  private long parse(String input) {
    Matcher matcher = PATTERN.matcher(input.trim());

    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid byte size: " + input);
    }

    double number = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(3).toUpperCase();

    Long multiplier = UNIT_MULTIPLIERS.get(unit);
    if (multiplier == null) {
      throw new IllegalArgumentException("Unsupported byte unit: " + unit);
    }

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
    return new JsonPrimitive(original);
  }
}
