/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.api.parser;

import co.cask.wrangler.api.LazyNumber;
import co.cask.wrangler.api.annotations.PublicEvolving;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Class description here.
 */
@PublicEvolving
public class Numeric implements Token {
  private final LazyNumber value;

  public Numeric(LazyNumber value) {
    this.value = value;
  }

  @Override
  public LazyNumber value() {
    return value;
  }

  @Override
  public TokenType type() {
    return TokenType.NUMERIC;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.NUMERIC.name());
    object.addProperty("value", value);
    return object;
  }
}
