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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.util.List;

/**
 * Class description here.
 */
@PublicEvolving
public class NumericList implements Token {
  private final List<LazyNumber> values;

  public NumericList(List<LazyNumber> values) {
    this.values = values;
  }

  @Override
  public List<LazyNumber> value() {
    return values;
  }

  @Override
  public TokenType type() {
    return TokenType.NUMERIC_LIST;
  }

  @Override
  public JsonElement toJsonObject() {
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.NUMERIC_LIST.name());
    JsonArray array = new JsonArray();
    for (LazyNumber value : values) {
      array.add(new JsonPrimitive(value));
    }
    object.add("value", array);
    return object;
  }
}
