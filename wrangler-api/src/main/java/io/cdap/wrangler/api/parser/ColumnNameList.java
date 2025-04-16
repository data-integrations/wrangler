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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.util.ArrayList;
import java.util.List;

/**
 * Class description here.
 */
@PublicEvolving
public class ColumnNameList implements Token {
  private List<String> values = new ArrayList<>();

  public ColumnNameList(List<String> values) {
    this.values = values;
  }

  @Override
  public List<String> value() {
    return values;
  }

  @Override
  public TokenType type() {
    return TokenType.COLUMN_NAME_LIST;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.COLUMN_NAME_LIST.name());
    JsonArray array = new JsonArray();
    for (String value : values) {
      array.add(new JsonPrimitive(value));
    }
    object.add("value", array);
    return object;
  }
}
