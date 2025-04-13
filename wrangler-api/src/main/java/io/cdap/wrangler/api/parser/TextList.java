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

import java.util.List;

/**
 * Token representing a list of text values.
 */
public class TextList {
  /** The list of text values. */
  private final List<String> values;

  /**
   * Creates a new text list token.
   *
   * @param values List of text values
   */
  public TextList(final List<String> values) {
    this.values = values;
  }

  /**
   * Gets the text values.
   *
   * @return List of text values
   */
  public final List<String> value() {
    return values;
  }

  /**
   * Gets the token type.
   *
   * @return Always TokenType.TEXT_LIST
   */
  public final TokenType type() {
    return TokenType.TEXT_LIST;
  }

  /**
   * Gets JSON representation.
   *
   * @return JSON string for this token
   */
  public final String toJson() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("{\"%s\":[", type().name().toLowerCase()));
    for (int i = 0; i < values.size(); i++) {
      sb.append(String.format("\"%s\"", values.get(i)));
      if (i != values.size() - 1) {
        sb.append(",");
      }
    }
    sb.append("]}");
    return sb.toString();
  }
}
