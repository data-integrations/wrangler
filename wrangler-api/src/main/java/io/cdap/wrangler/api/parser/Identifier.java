/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * The <code>Identifier</code> class wraps the primitive type {@code String} in a object.
 * An object of type {@code Identifier} contains the value in primitive type
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
 */
@PublicEvolving
public class Identifier implements Token {
  /**
   * The {@code String} object that represents the value held by the token.
   */
  private String value;

  /**
   * Allocates a {@code String} object representing the
   * {@code value} argument.
   *
   * @param value the value of the {@code String}.
   */
  public Identifier(String value) {
    this.value = value;
  }

  /**
   * Returns the value of this {@code String} object as a boolean
   * primitive.
   *
   * @return  the primitive {@code Identifier} value of this object.
   */
  @Override
  public String value() {
    return value;
  }

  /**
   * Returns the type of this {@code Identifier} object as a {@code TokenType}
   * enum.
   *
   * @return the enumerated {@code TokenType} of this object.
   */
  @Override
  public TokenType type() {
    return TokenType.IDENTIFIER;
  }

  /**
   * Returns the members of this {@code Identifier} object as a {@code JsonElement}.
   *
   * @return Json representation of this {@code Identifier} object as {@code JsonElement}
   */
  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("type", TokenType.IDENTIFIER.name());
    object.addProperty("value", value);
    return object;
  }
}
