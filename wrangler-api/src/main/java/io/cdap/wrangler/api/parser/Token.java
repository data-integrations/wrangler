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

/**
 * A token recognized during parsing, containing both the value and its type.
 */
public interface Token {
  /**
   * Gets the raw string value.
   *
   * @return Token value
   */
  String value();

  /**
   * Gets the token type.
   *
   * @return Token type
   */
  TokenType type();

  /**
   * Converts the token to JSON representation.
   *
   * @return JSON representation of the token
   */
  JsonElement toJson();
}
