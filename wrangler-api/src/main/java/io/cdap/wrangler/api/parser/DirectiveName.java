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

/**
 * Token representing a directive name.
 */
public class DirectiveName {
  /** The raw directive name value. */
  private final String value;

  /**
   * Creates a new directive name token.
   *
   * @param name The directive name
   */
  public DirectiveName(final String name) {
    this.value = name;
  }

  /**
   * Gets the directive name.
   *
   * @return The name value
   */
  public final String value() {
    return value;
  }

  /**
   * Gets the token type.
   *
   * @return Always TokenType.DIRECTIVE_NAME
   */
  public final TokenType type() {
    return TokenType.DIRECTIVE_NAME;
  }

  /**
   * Gets JSON representation.
   *
   * @return JSON string for this token
   */
  public final String toJson() {
    return String.format("{\"%s\":\"%s\"}", type().name().toLowerCase(), value);
  }
}
