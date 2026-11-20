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
  private final String rawValue;

  public ByteSize(String value) {
    this.rawValue = value.trim().toUpperCase();
  }

  public long getBytes() {
    if (rawValue.endsWith("KB")) {
      return (long)(Double.parseDouble(rawValue.replace("KB", "")) * 1024);
    } else if (rawValue.endsWith("MB")) {
      return (long)(Double.parseDouble(rawValue.replace("MB", "")) * 1024 * 1024);
    } else if (rawValue.endsWith("GB")) {
      return (long)(Double.parseDouble(rawValue.replace("GB", "")) * 1024 * 1024 * 1024);
    } else if (rawValue.endsWith("TB")) {
      return (long)(Double.parseDouble(rawValue.replace("TB", "")) * 1024L * 1024 * 1024 * 1024);
    } else {
      throw new IllegalArgumentException("Unknown byte size unit in: " + rawValue);
    }
  }

  @Override
  public Object value() {
    return rawValue;
  }

  @Override
  public TokenType type() {
    return TokenType.TEXT; // or define a new TokenType if needed, like BYTE_SIZE
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(rawValue);
  }

  @Override
  public String toString() {
    return rawValue;
  }
}
