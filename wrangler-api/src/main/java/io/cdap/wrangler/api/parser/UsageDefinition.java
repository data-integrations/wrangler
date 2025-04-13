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

import io.cdap.wrangler.api.Optional;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class {@link UsageDefinition} provides a way for users to registers the argument for UDDs.
 *
 * {@link UsageDefinition} is a collection of {@link TokenDefinition} and the name of the directive
 * itself. Each token specification has an associated ordinal that can be used to position the argument
 * within the directive.
 *
 * Following is a example of how this class can be used.
 * <code>
 *   UsageDefinition.Builder builder = UsageDefinition.builder();
 *   builder.add("col1", TypeToken.COLUMN_NAME); // By default, this field is required.
 *   builder.add("col2", TypeToken.COLUMN_NAME, false); // This is a optional field.
 *   builder.add("expression", TypeToken.EXPRESSION);
 *   UsageDefinition definition = builder.build();
 * </code>
 *
 * NOTE: No constraints checks are included in this implementation.
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
   * Returns the name of the directive for which the this <code>UsageDefinition</code>
   * object is created.
   *
   * @return name of the directive.
   */
  public String getDirectiveName() {
    return directive;
  }

  /**
   * This method returns the list of <code>TokenDefinition</code> that should be
   * used for parsing the directive into <code>Arguments</code>.
   *
   * @return List of <code>TokenDefinition</code>.
   */
  public List<TokenDefinition> getTokens() {
    return tokens;
  }

  /**
   * Returns the count of <code>TokenDefinition</code> that have been specified
   * as optional in the <code>UsageDefinition</code>.
   *
   * @return number of tokens in the usage that are optional.
   */
  public int getOptionalTokensCount() {
    return optionalCnt;
  }

  /**
   * This method converts the <code>UsageDefinition</code> into a usage string
   * for this directive. It inspects all the tokens to generate a standard syntax
   * for the usage of the directive.
   *
   * @return a usage representation of this object.
   */
  /**
 * Converts the UsageDefinition into a readable usage string format,
 * showing how each token (argument) should be written in the directive.
 * Handles optional tokens, token types, and labels.
 */
@Override
public String toString() {
  StringBuilder sb = new StringBuilder();

  // Start the usage string with the directive name
  sb.append(directive).append(" ");

  int count = tokens.size(); // Used to manage spacing between tokens
  for (TokenDefinition token : tokens) {

    // If token is optional, wrap it in square brackets
    if (token.optional()) {
      sb.append(" [");
    }

    // If a custom label is set, use it directly
    if (token.label() != null) {
      sb.append(token.label());
    } else {
      // Otherwise, build the token display based on its TokenType
      switch (token.type()) {

        // Directive name is shown as-is
        case DIRECTIVE_NAME:
          sb.append(token.name());
          break;

        // Column name prefixed with colon
        case COLUMN_NAME:
          sb.append(":").append(token.name());
          break;

        // List of columns with repetition pattern
        case COLUMN_NAME_LIST:
          sb.append(":").append(token.name()).append(" [,:").append(token.name()).append("  ]*");
          break;

        // Boolean flag with values true/false
        case BOOLEAN:
          sb.append(token.name()).append(" (true/false)");
          break;

        // Text wrapped in single quotes
        case TEXT:
          sb.append("'").append(token.name()).append("'");
          break;

        // Identifier or number shown directly
        case IDENTIFIER:
        case NUMERIC:
          sb.append(token.name());
          break;

        // Lists of values with repetition
        case BOOLEAN_LIST:
        case NUMERIC_LIST:
        case TEXT_LIST:
          sb.append(token.name()).append("[,").append(token.name()).append(" ...]*");
          break;

        // Expression enclosed in exp:{<...>}
        case EXPRESSION:
          sb.append("exp:{<").append(token.name()).append(">}");
          break;

        // Property map format
        case PROPERTIES:
          sb.append("prop:{key:value,[key:value]*");
          break;

        // Range pattern with optional types
        case RANGES:
          sb.append("start:end=[bool|text|numeric][,start:end=[bool|text|numeric]*");
          break;

        // NEW: Byte size values like 10KB, 2MB, etc.
        case BYTE_SIZE:
          sb.append(token.name()).append(" (e.g. 10KB, 2MB, 1GB)");
          break;

        // NEW: Time duration values like 100ms, 2s, etc.
        case TIME_DURATION:
          sb.append(token.name()).append(" (e.g. 100ms, 2s, 5m)");
          break;
      }
    }

    count--; // Reduce remaining token count

    // Close optional token with ']', else add space if more tokens exist
    if (token.optional()) {
      sb.append("]");
    } else {
      if (count > 0) {
        sb.append(" ");
      }
    }
  }

  // Return the full usage string
  return sb.toString();
}

  /**
   * This is a static method for creating a builder for the <code>UsageDefinition</code>
   * object. In order to create a <code>UsageDefinition</code>, a builder has to created.
   *
   * <p>This builder is provided as user API for constructing the usage specification
   * for a directive.</p>
   *
   * @param directive name of the directive for which the builder is created for.
   * @return A <code>UsageDefinition.Builder</code> object that can be used to construct
   * <code>UsageDefinition</code> object.
   */
  public static UsageDefinition.Builder builder(String directive) {
    return new UsageDefinition.Builder(directive);
  }

  /**
   * This inner builder class provides a way to create <code>UsageDefinition</code>
   * object. It exposes different methods that allow users to configure the <code>TokenDefinition</code>
   * for each token used within the usage of a directive.
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
     * This method provides a way to set the name and the type of token, while
     * defaulting the label to 'null' and setting the optional to FALSE.
     *
     * @param name of the token in the definition of a directive.
     * @param type of the token to be extracted.
     */
    public void define(String name, TokenType type) {
      TokenDefinition spec = new TokenDefinition(name, type, null, currentOrdinal, Optional.FALSE);
      currentOrdinal++;
      tokens.add(spec);
    }

    /**
     * Allows users to define a token with a name, type of the token and additional optional
     * for the label that is used during creation of the usage for the directive.
     *
     * @param name of the token in the definition of a directive.
     * @param type of the token to be extracted.
     * @param label label that modifies the usage for this field.
     */
    public void define(String name, TokenType type, String label) {
      TokenDefinition spec = new TokenDefinition(name, type, label, currentOrdinal, Optional.FALSE);
      currentOrdinal++;
      tokens.add(spec);
    }

    /**
     * Method allows users to specify a field as optional in combination to the
     * name of the token and the type of token.
     *
     * @param name of the token in the definition of a directive.
     * @param type of the token to be extracted.
     * @param optional <code>Optional#TRUE</code> if token is optional, else <code>Optional#FALSE</code>.
     */
    public void define(String name, TokenType type, boolean optional) {
      TokenDefinition spec = new TokenDefinition(name, type, null, currentOrdinal, optional);
      optionalCnt = optional ? optionalCnt + 1 : optionalCnt;
      currentOrdinal++;
      tokens.add(spec);
    }

    /**
     * Method allows users to specify a field as optional in combination to the
     * name of the token, the type of token and also the ability to specify a label
     * for the usage.
     *
     * @param name of the token in the definition of a directive.
     * @param type of the token to be extracted.
     * @param label label that modifies the usage for this field.
     * @param optional <code>Optional#TRUE</code> if token is optional, else <code>Optional#FALSE</code>.
     */
    public void define(String name, TokenType type, String label, boolean optional) {
      TokenDefinition spec = new TokenDefinition(name, type, label, currentOrdinal, optional);
      optionalCnt = optional ? optionalCnt + 1 : optionalCnt;
      currentOrdinal++;
      tokens.add(spec);
    }

    /**
     * @return a instance of <code>UsageDefinition</code> object.
     */
    public UsageDefinition build() {
      return new UsageDefinition(directive, optionalCnt, tokens);
    }
  }
}
