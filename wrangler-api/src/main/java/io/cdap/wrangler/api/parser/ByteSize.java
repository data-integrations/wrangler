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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * Token implementation for parsing byte size values like "10KB", "1.5MB", "2GB", etc.
 */
@PublicEvolving
public class ByteSize implements Token {
  private final long bytes;
  private final String original;

  public ByteSize(String value) {
    this.original = value;
    String lower = value.trim().toLowerCase();
    double numericValue;

    if (lower.endsWith("kb")) {
      numericValue = Double.parseDouble(lower.replace("kb", ""));
      this.bytes = (long) (numericValue * 1024);
    } else if (lower.endsWith("mb")) {
      numericValue = Double.parseDouble(lower.replace("mb", ""));
      this.bytes = (long) (numericValue * 1024 * 1024);
    } else if (lower.endsWith("gb")) {
      numericValue = Double.parseDouble(lower.replace("gb", ""));
      this.bytes = (long) (numericValue * 1024 * 1024 * 1024);
    } else if (lower.endsWith("tb")) {
      numericValue = Double.parseDouble(lower.replace("tb", ""));
      this.bytes = (long) (numericValue * 1024L * 1024 * 1024 * 1024);
    } else if (lower.endsWith("b")) {
      numericValue = Double.parseDouble(lower.replace("b", ""));
      this.bytes = (long) numericValue;
    } else {
      throw new IllegalArgumentException("Unsupported byte unit. Supported units: B, KB, MB, GB, TB. Input: " + value);
    }
  }

  public long getBytes() {
    return bytes;
  }

  @Override
  public Object value() {
    return original;
  }

  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(original);
  }

  @Override
  public String toString() {
    return bytes + " bytes";
  }
}
