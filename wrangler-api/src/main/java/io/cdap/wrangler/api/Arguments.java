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
 * This class {@code Arguments} represents the wrapped tokens that
 * are tokenized and parsed arguments provided to the {@code Executor}.
 *
 * This class <code>Arguments</code> includes methods for retrieving
 * the value of the token provided the name for the token, number of
 * tokens, support for checking if the named argument exists, type of
 * token as specified by <code>TokenType</code> and helper method for
 * constructing <code>JsonElement</code> object.
 *
 * @see io.cdap.wrangler.api.parser.UsageDefinition
 */
public interface Arguments {
  /**
   * This method returns the token {@code value} based on the {@code name}
   * specified in the argument. This method will attempt to convert the token
   * into the expected return type <code>T</code>.
   *
   * <p>If the <code>name</code> doesn't exist in this object, then this
   * method is expected to return <code>null</code></p>
   *
   * @param name of the token to be retrieved.
   * @param <T> type the token need to casted to.
   * @return object that extends <code>Token</code>.
   */
  <T extends Token> T value(String name);

  /**
   * Returns the number of tokens that are mapped to arguments.
   *
   * <p>The optional arguments specified during the <code>UsageDefinition</code>
   * are not included in the size if they are not present in the tokens parsed.</p>
   *
   * @return number of tokens parsed, excluding optional tokens if not present.
   */
  int size();

  /**
   * This method checks if there exists a token named <code>name</code> registered
   * with this object.
   *
   * The <code>name</code> is expected to the same as specified in the <code>UsageDefinition</code>.
   * There are two reason why the <code>name</code> might not exists in this object :
   *
   * <ul>
   *   <li>When an token is defined to be optional, the user might not have specified the
   *   token, hence the token would not exist in the argument.</li>
   *   <li>User has specified invalid <code>name</code>.</li>
   * </ul>
   *
   * @param name associated with the token.
   * @return true if argument with name <code>name</code> exists, false otherwise.
   */
  boolean contains(String name);

  /**
   * Each token is defined as one of the types defined in the class {@link TokenType}.
   * When the directive is parsed into token, the type of the token is passed through.
   *
   * @param name associated with the token.
   * @return <code>TokenType</code> associated with argument <code>name</code>, else null.
   */
  TokenType type(String name);

  /**
   * Returns the source line number these arguments were parsed from.
   *
   * @return the source line number.
   */
  int line();

  /**
   * Returns the source column number these arguments were parsed from.
   * <p>It takes the start position of the directive as the column number.</p>
   *
   * @return the start of the column number for the start of the directive
   * these arguments contain.
   */
  int column();

  /**
   * This method returns the original source line of the directive as specified
   * the user. It returns the <code>String</code> representation of the directive.
   *
   * @return <code>String</code> object representing the original directive
   * as specified by the user.
   */
  String source();

  /**
   * Returns <code>JsonElement</code> representation of this object.
   *
   * @return an instance of <code>JsonElement</code>object representing all the
   * named tokens held within this object.
   */
  JsonElement toJson();
}
