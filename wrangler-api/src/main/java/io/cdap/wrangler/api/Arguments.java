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

package io.cdap.wrangler.api;

import com.google.gson.JsonElement;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenType;

/**
 * Represents parsed and tokenized arguments provided to an {@link Executor}.
 * This interface provides methods for accessing argument values, checking argument 
 * existence and types, and retrieving source position information.
 *
 * <p>Arguments are defined in {@link io.cdap.wrangler.api.parser.UsageDefinition} 
 * and correspond to the tokens parsed from directive inputs.</p>
 *
 * @see io.cdap.wrangler.api.parser.UsageDefinition
 */
public interface Arguments {
  /**
   * Gets a token value by name and converts it to the expected type.
   *
   * @param name Name of the token to retrieve as defined in UsageDefinition
   * @param <T> Expected token type that extends Token
   * @return The token value cast to type T, or null if the named token doesn't exist
   * @throws ClassCastException if the token cannot be cast to type T
   */
  <T extends Token> T value(String name);

  /**
   * Gets the number of actual tokens parsed from the directive input.
   * Optional tokens that were not provided are not included in this count.
   *
   * @return Number of non-optional tokens successfully parsed
   */
  int size();

  /**
   * Checks if a named token exists in the parsed arguments.
   *
   * <p>A token may not exist either because:</p>
   * <ul>
   *   <li>It was defined as optional in UsageDefinition and not provided</li>
   *   <li>The provided name does not match any defined token</li>
   * </ul>
   *
   * @param name Name of the token to check for
   * @return true if a token with the given name exists, false otherwise
   */
  boolean contains(String name);

  /**
   * Gets the TokenType of a named argument.
   *
   * @param name Name of the token whose type to retrieve
   * @return The TokenType of the named token, or null if the token doesn't exist
   */
  TokenType type(String name);

  /**
   * Gets the source line number where these arguments were parsed from.
   *
   * @return The 1-based line number in the source
   */
  int line();

  /**
   * Gets the source column number where the directive containing these arguments starts.
   *
   * @return The 1-based column number marking the start of the directive
   */
  int column();

  /**
   * Converts the arguments to a JSON representation.
   *
   * @return JsonElement containing the arguments' data
   */
  JsonElement toJson();
}
