/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.wrangler.api.parser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import io.cdap.wrangler.api.Optional;

/**
 * UsageDefinition provides a way for users to register arguments for UDDs.
 * It is a collection of TokenDefinition objects and the name of the directive.
 * Each token specification has an associated ordinal that is used for positioning the argument
 * within the directive.
 *
 * Example usage:
 * <code>
 *   UsageDefinition.Builder builder = UsageDefinition.builder("my-directive");
 *   builder.define("col1", TokenType.COLUMN_NAME); // Required field.
 *   builder.define("col2", TokenType.COLUMN_NAME, false); // Optional field.
 *   builder.define("expression", TokenType.EXPRESSION);
 *   UsageDefinition definition = builder.build();
 * </code>
 *
 * NOTE: No constraint checks are included in this implementation.
 *
 * @see TokenDefinition
 */
public final class UsageDefinition implements Serializable {

  // transient so it doesn't show up when serialized using gson in service endpoint responses
  private final transient int optionalCnt;
  private final String directive;
  private final List<TokenDefinition> tokens;

  private UsageDefinition(String directive, int optionalCnt, List<TokenDefinition> tokens) {
    this.directive = directive;
    this.tokens = tokens;
    this.optionalCnt = optionalCnt;
  }

  /**
   * Returns the name of the directive.
   *
   * @return the directive name.
   */
  public String getDirectiveName() {
    return directive;
  }

  /**
   * Returns the list of TokenDefinition objects used for parsing the directive.
   *
   * @return the list of TokenDefinition objects.
   */
  public List<TokenDefinition> getTokens() {
    return tokens;
  }

  /**
   * Returns the number of optional tokens in the usage.
   *
   * @return the count of optional tokens.
   */
  public int getOptionalTokensCount() {
    return optionalCnt;
  }

  /**
   * Converts the UsageDefinition into a usage string.
   *
   * @return a usage representation of the directive.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(directive).append(" ");

    int count = tokens.size();
    for (TokenDefinition token : tokens) {
      if (token.optional()) {
        sb.append(" [");
      }

      if (token.label() != null) {
        sb.append(token.label());
      } else {
        // Use a switch instead of a chain of if-else statements for clarity.
        switch (token.type()) {
          case DIRECTIVE_NAME:
            sb.append(token.name());
            break;
          case COLUMN_NAME:
            sb.append(":").append(token.name());
            break;
          case COLUMN_NAME_LIST:
            sb.append(":").append(token.name()).append(" [,:").append(token.name()).append("  ]*");
            break;
          case BOOLEAN:
            sb.append(token.name()).append(" (true/false)");
            break;
          case TEXT:
            sb.append("'").append(token.name()).append("'");
            break;
          case IDENTIFIER:
          case NUMERIC:
            sb.append(token.name());
            break;
          case BOOLEAN_LIST:
          case NUMERIC_LIST:
          case TEXT_LIST:
            sb.append(token.name()).append("[,").append(token.name()).append(" ...]*");
            break;
          case EXPRESSION:
            sb.append("exp:{<").append(token.name()).append(">}");
            break;
          case PROPERTIES:
            sb.append("prop:{key:value,[key:value]*");
            break;
          case RANGES:
            sb.append("start:end=[bool|text|numeric][,start:end=[bool|text|numeric]*");
            break;
          default:
            sb.append(token.name());
            break;
        }
      }

      count--;

      if (token.optional()) {
        sb.append("]");
      } else {
        if (count > 0) {
          sb.append(" ");
        }
      }
    }
    return sb.toString();
  }

  /**
   * Returns a builder for creating a UsageDefinition.
   *
   * @param directive the directive name.
   * @return a Builder instance.
   */
  public static Builder builder(String directive) {
    return new Builder(directive);
  }

  /**
   * The Builder for UsageDefinition.
   */
  public static final class Builder {
    private final String directive;
    private final List<TokenDefinition> tokens;
    private int currentOrdinal;
    private int optionalCnt;

    public Builder(String directive) {
      this.directive = directive;
      this.currentOrdinal = 0;
      this.tokens = new ArrayList<>();
      this.optionalCnt = 0;
    }

    /**
     * Sets the name and type of token, defaulting label to null and optional to FALSE.
     *
     * @param name the token's name.
     * @param type the token's type.
     * @return the Builder instance (for chaining).
     */
    public Builder define(String name, TokenType type) {
      TokenDefinition spec = new TokenDefinition(name, type, null, currentOrdinal, Optional.FALSE);
      currentOrdinal++;
      tokens.add(spec);
      return this;
    }

    /**
     * Defines a token with a name, type, and label.
     *
     * @param name the token's name.
     * @param type the token's type.
     * @param label the token's label.
     * @return the Builder instance (for chaining).
     */
    public Builder define(String name, TokenType type, String label) {
      TokenDefinition spec = new TokenDefinition(name, type, label, currentOrdinal, Optional.FALSE);
      currentOrdinal++;
      tokens.add(spec);
      return this;
    }

    /**
     * Defines a token with a name and type, and specifies if it's optional.
     *
     * @param name the token's name.
     * @param type the token's type.
     * @param optional whether the token is optional.
     * @return the Builder instance (for chaining).
     */
    public Builder define(String name, TokenType type, boolean optional) {
      TokenDefinition spec = new TokenDefinition(name, type, null, currentOrdinal, optional);
      if (optional) {
        optionalCnt++;
      }
      currentOrdinal++;
      tokens.add(spec);
      return this;
    }

    /**
     * Defines a token with a name, type, label, and an optional flag.
     *
     * @param name the token's name.
     * @param type the token's type.
     * @param label the token's label.
     * @param optional whether the token is optional.
     * @return the Builder instance (for chaining).
     */
    public Builder define(String name, TokenType type, String label, boolean optional) {
      TokenDefinition spec = new TokenDefinition(name, type, label, currentOrdinal, optional);
      if (optional) {
        optionalCnt++;
      }
      currentOrdinal++;
      tokens.add(spec);
      return this;
    }

    /**
     * Builds the UsageDefinition.
     *
     * @return an instance of UsageDefinition.
     */
    public UsageDefinition build() {
      return new UsageDefinition(directive, optionalCnt, tokens);
    }
  }
}