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
 * The Bool class wraps the primitive type {@code Boolean} in a object.
 * An object of type {@code Bool} contains the value in primitive type
 * as well as the type of the token this class represents.
 *
 * <p>In addition, this class provides two methods one to extract the
 * value held by this wrapper object, and the second for extracting the
 * type of the token.</p>
 *
 * @see BoolList
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
 * @see Identifier
 */
@PublicEvolving
public class Bool implements Token {
  private Boolean value;

  public Bool(Boolean value) {
    this.value = value;
  }

  @Override
  public String value() {
    return value.toString();
  }

  @Override
  public TokenType type() {
    return TokenType.BOOLEAN;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.BOOLEAN.name());
    object.addProperty("value", value);
    return object;
  }
}
