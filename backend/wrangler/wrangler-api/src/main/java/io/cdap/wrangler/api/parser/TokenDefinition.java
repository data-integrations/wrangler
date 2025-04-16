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
 *   TokenDefinition token = new TokenDefintion("column", TokenType.COLUMN_NAME, null, 0, Optional.FALSE);
 * </code>
 *
 * <p>The class <code>TokenDefinition</code> includes methods for retrieveing different members of
 * like name of the token, type of the token, label associated with token, whether it's optional or not
 * and the ordinal number of the token in the <code>TokenGroup</code>.</p>
 *
 * <p>As this class is immutable, the constructor requires all the member variables to be presnted
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
   * @return Label associated with the token. Label provides a way to override the usage description
   * for this <code>TokenDefinition</code>. If a label is not provided, then this return null.
   */
  public String label() {
    return label;
  }

  /**
   * @return Returns the oridinal number of this <code>TokenDefinition</code> within
   * the <code>TokenGroup</code>,
   */
  public int ordinal() {
    return ordinal;
  }

  /**
   * @return true, if this <code>TokenDefinition</code> is optional, false otherwise.
   */
  public boolean optional() {
    return optional;
  }

  /**
   * @return Name of this <code>TokenDefinition</code>
   */
  public String name() {
    return name;
  }

  /**
   * @return Returns the type of this <code>TokenDefinition</code>.
   */
  public TokenType type() {
    return type;
  }

}
