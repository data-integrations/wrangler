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

import java.util.List;

/**
 * The Bool List class wraps the list of primitive type {@code Boolean} in a object.
 * An object of type {@code BoolList} contains the value as a {@code List} of primitive type
 * {@code Boolean}. Along with the list of {@code Boolean} type, this object also contains
 * the value that represents the type of this object as {@code TokenType}.
 *
 * <p>In addition, this class provides two methods one to extract the
 * value held by this wrapper object, and the second for extracting the
 * type of the token.</p>
 *
 * @see Bool
 * @see ColumnName
 * @see ColumnNameList
 * @see DirectiveName
 * @see Numeric
 * @see NumericList
 * @see Properties
 * @see Ranges
 * @see Expression
 * @see Text
 * @see TextList
 */
@PublicEvolving
public class BoolList implements Token {
  private List<Boolean> values;

  public BoolList(List<Boolean> values) {
    this.values = values;
  }

  @Override
  public String value() {
    return values.toString();
  }

  @Override
  public TokenType type() {
    return TokenType.BOOLEAN_LIST;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.BOOLEAN_LIST.name());
    JsonArray array = new JsonArray();
    for (Boolean value : values) {
      array.add(new JsonPrimitive(value));
    }
    object.add("value", array);
    return object;
  }
}
