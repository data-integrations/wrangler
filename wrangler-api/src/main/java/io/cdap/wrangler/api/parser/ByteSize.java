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
import com.google.gson.JsonPrimitive;

public class ByteSize implements Token {
  private final String raw;
  private final long bytes;

  public ByteSize(String value) {
    this.raw = value;
    this.bytes = parse(value);
  }

  private long parse(String input) {
    input = input.toUpperCase();
    double number = Double.parseDouble(input.replaceAll("[^0-9.]", ""));
    if (input.endsWith("KB")) return (long)(number * 1024);
    if (input.endsWith("MB")) return (long)(number * 1024 * 1024);
    if (input.endsWith("GB")) return (long)(number * 1024 * 1024 * 1024);
    if (input.endsWith("TB")) return (long)(number * 1024L * 1024 * 1024 * 1024);
    if (input.endsWith("B")) return (long) number;
    return (long) number;
  }

  public long getBytes() {
    return bytes;
  }

  @Override
  public Object value() {
    return raw;
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(raw);
  }
}