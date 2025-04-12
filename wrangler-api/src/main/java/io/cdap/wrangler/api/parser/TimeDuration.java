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
 * Token implementation for TimeDuration like "500ms", "2s", "5m", etc.
 */
@PublicEvolving
public class TimeDuration implements Token {
  private final long millis;
  private final String original;

  public TimeDuration(String value) {
    this.original = value.trim().toLowerCase();
    System.out.println("Parsing duration: " + original);
    this.millis = parseMillis(original);
  }

  private long parseMillis(String value) {
    if (value.endsWith("ns")) {
      return (long)(Double.parseDouble(value.replace("ns", "")) / 1_000_000); // convert ns to ms
    } else if (value.endsWith("ms")) {
      return (long)(Double.parseDouble(value.replace("ms", "")));
    } else if (value.endsWith("s")) {
      return (long)(Double.parseDouble(value.replace("s", "")) * 1000);
    } else if (value.endsWith("m")) {
      return (long)(Double.parseDouble(value.replace("m", "")) * 60 * 1000);
    } else if (value.endsWith("h")) {
      return (long)(Double.parseDouble(value.replace("h", "")) * 60 * 60 * 1000);
    } else if (value.endsWith("d")) {
      return (long)(Double.parseDouble(value.replace("d", "")) * 24 * 60 * 60 * 1000);
    } else {
      throw new IllegalArgumentException("Invalid time duration format: " + value);
    }
  }
  
  public long getNanos() {
    return (long)(Double.parseDouble(original.replaceAll("[a-z]+", "")) *
      (original.endsWith("ns") ? 1 :
       original.endsWith("ms") ? 1_000_000 :
       original.endsWith("s")  ? 1_000_000_000 :
       original.endsWith("m")  ? 60L * 1_000_000_000 :
       original.endsWith("h")  ? 3600L * 1_000_000_000 :
       original.endsWith("d")  ? 86400L * 1_000_000_000 : 0));
  }
  
  

  public long getMillis() {
    return millis;
  }
  

  @Override
  public Object value() {
    return millis;
  }

  @Override
  public TokenType type() {
    return TokenType.TIME_DURATION;
  }

  @Override
  public JsonElement toJson() {
    JsonObject json = new JsonObject();
    json.addProperty("type", TokenType.TIME_DURATION.name());
    json.addProperty("value", millis);
    return json;
  }
}
