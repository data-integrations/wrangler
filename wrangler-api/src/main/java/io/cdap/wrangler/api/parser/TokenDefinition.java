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

import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.io.Serializable;

/**
 * The <code>TokenDefinition</code> class represents a definition of token as specified
 * by the user while defining a directive usage. All definitions of a token are represented
 * by a instance of this class.
 *
 * The definition are constant (immutable) and they cannot be changed once defined.
 * For example :
 * <code>
 *   TokenDefinition token = new TokenDefinition("column", TokenType.COLUMN_NAME, null, 0, false);
 * </code>
 *
 * <p>The class <code>TokenDefinition</code> includes methods for retrieving different members of
 * like name of the token, type of the token, label associated with token, whether it's optional or not
 * and the ordinal number of the token in the <code>TokenGroup</code>.</p>
 *
 * <p>As this class is immutable, the constructor requires all the member variables to be presented
 * for an instance of this object to be created.</p>
 */
@PublicEvolving
public final class TokenDefinition implements Serializable {
  private final int ordinal;
  private final boolean optional;
  private final String name;
  private final TokenType type;
  private final String label;

  public TokenDefinition(String name, TokenType type, String label, int ordinal, boolean optional) {
    this.name = name;
    this.type = type;
    this.label = label;
    this.ordinal = ordinal;
    this.optional = optional;
  }

  /**
   * Returns the label associated with the token.
   *
   * @return Label for usage description, or null if not provided.
   */
  public String label() {
    return label;
  }

  /**
   * Returns the ordinal number of this token definition.
   *
   * @return Ordinal position within the <code>TokenGroup</code>.
   */
  public int ordinal() {
    return ordinal;
  }

  /**
   * Checks if this token definition is optional.
   *
   * @return true if optional, false otherwise.
   */
  public boolean optional() {
    return optional;
  }

  /**
   * Returns the name of this token definition.
   *
   * @return Name of the token.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the type of this token definition.
   *
   * @return The <code>TokenType</code> of the token.
   */
  public TokenType type() {
    return type;
  }

  /**
   * Returns a string representation of this token definition.
   *
   * @return String describing the token, with examples for BYTE_SIZE and TIME_DURATION.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TokenDefinition{name='");
    sb.append(name).append("', type=").append(type);
    if (type == TokenType.BYTE_SIZE) {
      sb.append(" [e.g., 10kb]");
    } else if (type == TokenType.TIME_DURATION) {
      sb.append(" [e.g., 150ms]");
    }
    if (label != null) {
      sb.append(", label='").append(label).append("'");
    }
    sb.append(", ordinal=").append(ordinal);
    sb.append(", optional=").append(optional).append("}");
    return sb.toString();
  }
}

