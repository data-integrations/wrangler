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

/**
 * A mock implementation of the Token interface for testing purposes.
 */
public class MockToken implements Token {
  private final TokenType type;
  private final Object value;

  public MockToken(TokenType type, Object value) {
    this.type = type;
    this.value = value;
  }

  @Override
  public TokenType type() {
    return type;
  }

  @Override
  public Object value() {
    return value;
  }

  @Override
  public JsonElement toJson() {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("type", type.name());
    jsonObject.addProperty("value", value.toString());
    return jsonObject;
  }
}
