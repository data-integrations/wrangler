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

import com.google.gson.JsonObject;
import io.cdap.wrangler.api.LazyNumber;

/**
 * A {@link Token} for representing byte sizes with units (e.g., KB, MB, GB).
 */
public class ByteSize implements Token {
  private final LazyNumber value;
  private final String unit;

  public ByteSize(LazyNumber value, String unit) {
    this.value = value;
    this.unit = unit;
  }

  /**
   * @return numeric value of the byte size
   */
  public LazyNumber value() {
    return value;
  }

  /**
   * @return unit of the byte size (KB, MB, GB, etc.)
   */
  public String unit() {
    return unit;
  }

  /**
   * @return the type of the token
   */
  @Override
  public TokenType type() {
    return TokenType.BYTE_SIZE;
  }

  /**
   * @return JSON representation of the token
   */
  @Override
  public JsonObject toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", type().name());
    object.addProperty("value", value.toString());
    object.addProperty("unit", unit);
    return object;
  }
} 