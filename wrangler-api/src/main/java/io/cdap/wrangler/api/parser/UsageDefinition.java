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

import io.cdap.wrangler.api.Optional;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides a way to register arguments for user-defined directives (UDDs).
 * <p>
 * A {@code UsageDefinition} is a collection of {@link TokenDefinition} objects and the name
 * of the directive itself. Each token has an associated ordinal that positions the argument
 * within the directive.
 * </p>
 * <p>
 * Example usage:
 * <pre>
 * UsageDefinition.Builder builder = UsageDefinition.builder("directive");
 * builder.define("col1", TokenType.COLUMN_NAME); // Required by default
 * builder.define("col2", TokenType.COLUMN_NAME, false); // Optional
 * builder.define("expression", TokenType.EXPRESSION);
 * UsageDefinition definition = builder.build();
 * </pre>
 * </p>
 * <p>
 * Note: This implementation does not include constraint checks.
 * </p>
 *
 * @see TokenDefinition
 */
public final class UsageDefinition implements Serializable {
  // Transient to exclude from serialization in service endpoint responses
  private final transient int optionalCnt;
  private final String directive;
  private final List<TokenDefinition> tokens;
  private final int byteSize;
  private final int timeDuration;

  private UsageDefinition(String directive, int optionalCnt, List<TokenDefinition> tokens) {
    this.directive = directive;
    this.tokens = tokens;
    this.optionalCnt = optionalCnt;
    this.byteSize = 0;
    this.timeDuration = 0;
  }

  /**
   * Returns the name of the directive for which this {@code UsageDefinition} is created.
   *
   * @return the directive name
   */
  public String getDirectiveName() {
    return directive;
  }

  /**
   * Returns the list of {@code TokenDefinition} objects used to parse the directive into arguments.
   *
   * @return the list of token definitions
   */
  public List<TokenDefinition> getTokens() {
    return tokens;
  }

  /**
   * Returns the count of optional {@code TokenDefinition} objects in this {@code UsageDefinition}.
   *
   * @return the number of optional tokens
   */
  public int getOptionalTokensCount() {
    return optionalCnt;
  }

  /**
   * Converts this {@code UsageDefinition} into a usage string for the directive.
   * <p>
   * Inspects all tokens to generate a standard syntax for the directive's usage.
   * </p>
   *
   * @return a string representing the usage
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
        if (token.type().equals(TokenType.DIRECTIVE_NAME)) {
          sb.append(token.name());
        } else if (token.type().equals(TokenType.COLUMN_NAME)) {
          sb.append(":").append(token.name());
        } else if (token.type().equals(TokenType.COLUMN_NAME_LIST)) {
          sb.append(":").append(token.name()).append(" [,:").append(token.name()).append("  ]*");
        } else if (token.type().equals(TokenType.BOOLEAN)) {
          sb.append(token.name()).append(" (true/false)");
        } else if (token.type().equals(TokenType.TEXT)) {
          sb.append("'").append(token.name()).append("'");
        } else if (token.type().equals(TokenType.IDENTIFIER) || token.type().equals(TokenType.NUMERIC)) {
          sb.append(token.name());
        } else if (token.type().equals(TokenType.BOOLEAN_LIST) || token.type().equals(TokenType.NUMERIC_LIST)
          || token.type().equals(TokenType.TEXT_LIST)) {
          sb.append(token.name()).append("[,").append(token.name()).append(" ...]*");
        } else if (token.type().equals(TokenType.EXPRESSION)) {
          sb.append("exp:{<").append(token.name()).append(">}");
        } else if (token.type().equals(TokenType.PROPERTIES)) {
          sb.append("prop:{key:value,[key:value]*");
        } else if (token.type().equals(TokenType.RANGES)) {
          sb.append("start:end=[bool|text|numeric][,start:end=[bool|text|numeric]*");
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
   * Creates a builder for constructing a {@code UsageDefinition}.
   *
   * @param directive the name of the directive
   * @return a {@code Builder} for the directive
   */
  public static Builder builder(String directive) {
    return new Builder(directive);
  }

  /**
   * Builder class for creating a {@code UsageDefinition}.
   * <p>
   * Provides methods to configure {@code TokenDefinition} objects for tokens used in a directive.
   * </p>
   */
  public static final class Builder {
    private final String directive;
    private final List<TokenDefinition> tokens;
    private int currentOrdinal;
    private int optionalCnt;

    private Builder(String directive) {
      this.directive = directive;
      this.currentOrdinal = 0;
      this.tokens = new ArrayList<>();
      this.optionalCnt = 0;
    }

    /**
     * Defines a required token with a name and type, using a null label.
     *
     * @param name the token name
     * @param type the token type
     */
    public void define(String name, TokenType type) {
      TokenDefinition spec = new TokenDefinition(name, type, null, currentOrdinal, Optional.FALSE);
      currentOrdinal++;
      tokens.add(spec);
    }

    /**
     * Defines a required token with a name, type, and usage label.
     *
     * @param name the token name
     * @param type the token type
     * @param label the label for usage description
     */
    public void define(String name, TokenType type, String label) {
      TokenDefinition spec = new TokenDefinition(name, type, label, currentOrdinal, Optional.FALSE);
      currentOrdinal++;
      tokens.add(spec);
    }

    /**
     * Defines a token with a name, type, and optional status.
     *
     * @param name the token name
     * @param type the token type
     * @param optional true if the token is optional, false otherwise
     */
    public void define(String name, TokenType type, boolean optional) {
      TokenDefinition spec = new TokenDefinition(name, type, null, currentOrdinal, optional);
      optionalCnt = optional ? optionalCnt + 1 : optionalCnt;
      currentOrdinal++;
      tokens.add(spec);
    }

    /**
     * Defines a token with a name, type, usage label, and optional status.
     *
     * @param name the token name
     * @param type the token type
     * @param label the label for usage description
     * @param optional true if the token is optional, false otherwise
     */
    public void define(String name, TokenType type, String label, boolean optional) {
      TokenDefinition spec = new TokenDefinition(name, type, label, currentOrdinal, optional);
      optionalCnt = optional ? optionalCnt + 1 : optionalCnt;
      currentOrdinal++;
      tokens.add(spec);
    }

    /**
     * Builds the {@code UsageDefinition}.
     *
     * @return the constructed {@code UsageDefinition}
     */
    public UsageDefinition build() {
      return new UsageDefinition(directive, optionalCnt, tokens);
    }
  }
}
