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
import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.io.Serializable;

/**
 * The Token class represents the object that contains the value and type of
 * the token as parsed by the parser of the grammar defined for recipe.
 *
 * <p>This class provides methods for retrieving the wrapped value of token parsed
 * as well the type of token the implementation of this interface represents.</p>
 *
 * <p>It also provides method for providing the {@code JsonElement} of implementation
 * of this interface.</p>
 */
@PublicEvolving
public interface Token extends Serializable {
  /**
   * Returns the {@code value} of the object wrapped by the
   * implementation of this interface.
   *
   * @return {@code value} wrapped by the implementation of this interface.
   */
  Object value();

  /**
   * Returns the {@code TokenType} of the object represented by the
   * implementation of this interface.
   *
   * @return {@code TokenType} of the implementation object.
   */
  TokenType type();

  /**
   * The class implementing this interface will return the {@code JsonElement}
   * instance including the values of the object.
   *
   * @return {@code JsonElement} object containing members of  implementing class.
   */
  JsonElement toJson();
}
