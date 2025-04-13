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

package io.cdap.wrangler.api;

import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.UsageDefinition;
import java.util.ArrayList;
import java.util.List;

/**
 * A symbol table for tracking directives and their usage.
 *
 * <p>The symbol table maps directive names to their allowed usage patterns
 * defined by TokenGroups.
 *
 * <p>Usage Example:
 * <pre>
 * RecipeSymbol symbols = new RecipeSymbol();
 * symbols.addToken(new Token("name", TokenType.TEXT));
 * </pre>
 */
public class RecipeSymbol {
  /** Version of the recipe spec. */
  private final int version;

  /** List of loadable directives. */
  private final List<String> loadableDirectives;

  /** List of token groups. */
  private final List<TokenGroup> tokens;

  /**
   * Creates a new symbol table.
   *
   * @param version Version of recipe spec
   * @param loadableDirectives List of directives that can be loaded
   * @param tokens List of token groups defining directive usage
   */
  public RecipeSymbol(final int version, final List<String> loadableDirectives,
      final List<TokenGroup> tokens) {
    this.version = version;
    this.loadableDirectives = loadableDirectives;
    this.tokens = tokens;
  }

  /**
   * Creates a new empty symbol table.
   *
   * <p>Uses default version 1 and empty lists for directives and tokens.
   */
  public RecipeSymbol() {
    this(1, new ArrayList<>(), new ArrayList<>());
  }

  /**
   * Gets the recipe spec version.
   *
   * @return The version number
   */
  public int getVersion() {
    return version;
  }

  /**
   * Gets the list of loadable directives.
   *
   * @return List of directive names
   */
  public List<String> getLoadableDirectives() {
    return loadableDirectives;
  }

  /**
   * Gets all token groups.
   *
   * @return List of token groups
   */
  public List<TokenGroup> getTokens() {
    return tokens;
  }

  /**
   * Builder class for constructing a RecipeSymbol.
   */
  public static class Builder {
    /** The version to use. */
    private final int version;

    /** The list of directives. */
    private final List<String> directives;

    /** The list of token groups. */
    private final List<TokenGroup> tokens;

    /** The current token group being built. */
    private TokenGroup group;

    /**
     * Creates a new builder.
     *
     * @param version Recipe spec version
     */
    public Builder(final int version) {
      this.version = version;
      this.directives = new ArrayList<>();
      this.tokens = new ArrayList<>();
      this.group = null;
    }

    /**
     * Adds a directive to the loadable list.
     *
     * @param directive Name of directive to add
     * @return This builder instance
     */
    public Builder addLoadableDirective(final String directive) {
      directives.add(directive);
      return this;
    }

    /**
     * Starts a new token group.
     *
     * @param info Source info for the group
     * @return This builder instance
     */
    public Builder startUsage(final SourceInfo info) {
      group = new TokenGroup(info);
      return this;
    }

    /**
     * Adds a token to the current group.
     *
     * @param token Token to add
     * @return This builder instance
     */
    public Builder addToken(final Token token) {
      if (group != null) {
        group.addToken(token.value());
      }
      return this;
    }

    /**
     * Finishes the current token group.
     *
     * @return This builder instance
     */
    public Builder endUsage() {
      if (group != null) {
        tokens.add(group);
        group = null;
      }
      return this;
    }

    /**
     * Builds a new RecipeSymbol with the configured values.
     *
     * @return The constructed RecipeSymbol
     */
    public RecipeSymbol build() {
      return new RecipeSymbol(version, directives, tokens);
    }
  }
}
