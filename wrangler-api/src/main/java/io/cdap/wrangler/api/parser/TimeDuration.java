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

public class TimeDuration implements Token {
  private final String raw;
  private final long millis;

  public TimeDuration(String value) {
    this.raw = value;
    this.millis = parse(value);
  }

  private long parse(String input) {
    input = input.toLowerCase();
    double number = Double.parseDouble(input.replaceAll("[^0-9.]", ""));
    if (input.endsWith("ms")) return (long) number;
    if (input.endsWith("s")) return (long)(number * 1000);
    if (input.endsWith("m")) return (long)(number * 60 * 1000);
    if (input.endsWith("h")) return (long)(number * 60 * 60 * 1000);
    return 0;
  }

  public long getMilliseconds() {
    return millis;
  }

  @Override
  public Object value() {
    return raw;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(raw);
  }
}