/*
 * Copyright © 2017-2025 Cask Data, Inc.
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

import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.io.Serializable;

/**
 * Represents a definition of a token as specified by the user for directive usage.
 * <p>
 * All token definitions are immutable and cannot be changed once created.
 * For example:
 * <pre>
 * TokenDefinition token = new TokenDefinition("column", TokenType.COLUMN_NAME, null, 0, false);
 * </pre>
 * </p>
 * <p>
 * This class provides methods to retrieve the token's name, type, label, optional status,
 * and ordinal position within a {@code TokenGroup}.
 * </p>
 * <p>
 * As an immutable class, the constructor requires all member variables to be provided.
 * </p>
 */
@PublicEvolving
public final class TokenDefinition implements Serializable {
  private final int ordinal;
  private final boolean optional;
  private final String name;
  private final TokenType type;
  private final String label;
  private final int byteSize;
  private final int timeDuration;

  /**
   * Constructs a new {@code TokenDefinition}.
   *
   * @param name the name of the token
   * @param type the type of the token
   * @param label the label for usage description, or null if none
   * @param ordinal the ordinal position in the {@code TokenGroup}
   * @param optional whether the token is optional
   */
  public TokenDefinition(String name, TokenType type, String label, int ordinal, boolean optional) {
    this.name = name;
    this.type = type;
    this.label = label;
    this.ordinal = ordinal;
    this.optional = optional;
    this.byteSize = 0;
    this.timeDuration = 0;
  }

  /**
   * Returns the label associated with this token.
   * <p>
   * The label overrides the usage description. If no label is provided, this returns null.
   * </p>
   *
   * @return the label, or null if none
   */
  public String label() {
    return label;
  }

  /**
   * Returns the ordinal position of this token within a {@code TokenGroup}.
   *
   * @return the ordinal number
   */
  public int ordinal() {
    return ordinal;
  }

  /**
   * Checks if this token is optional.
   *
   * @return true if optional, false otherwise
   */
  public boolean optional() {
    return optional;
  }

  /**
   * Returns the name of this token.
   *
   * @return the token name
   */
  public String name() {
    return name;
  }

  /**
   * Returns the type of this token.
   *
   * @return the {@code TokenType}
   */
  public TokenType type() {
    return type;
  }
}
