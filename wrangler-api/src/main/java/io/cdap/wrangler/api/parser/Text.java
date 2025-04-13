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
 * Token representing a text value.
 */
public class Text {
  /** The text value. */
  private final String value;

  /**
   * Creates a new text token.
   *
   * @param value The text value
   */
  public Text(final String value) {
    this.value = value;
  }

  /**
   * Gets the text value.
   *
   * @return The text value
   */
  public final String value() {
    return value;
  }

  /**
   * Gets the token type.
   *
   * @return Always TokenType.TEXT
   */
  public final TokenType type() {
    return TokenType.TEXT;
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
