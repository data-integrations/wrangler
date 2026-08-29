/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.wrangler.api;

import com.google.gson.JsonElement;
import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * Interface representing a token in the Wrangler grammar.
 * Tokens are the basic building blocks of the grammar parser.
 */
@PublicEvolving
public interface Token {
  /**
   * Returns the value of the token.
   *
   * @return the token value
   */
  Object value();

  /**
   * Returns the type of the token.
   *
   * @return the token type
   */
  TokenType type();

  /**
   * Converts the token to a JSON element.
   *
   * @return JSON representation of the token
   */
  JsonElement toJson();
} 