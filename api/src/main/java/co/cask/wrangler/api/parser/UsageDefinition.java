/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.api.parser;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

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
public final class UsageDefinition {
  private String directive;
  private final List<TokenDefinition> tokens;

  private UsageDefinition(String directive, List<TokenDefinition> tokens) {
    this.directive = directive;
    this.tokens = tokens;
  }

  public String getDirective() {
    return directive;
  }

  public List<TokenDefinition> getTokens() {
    return tokens;
  }

  public static UsageDefinition.Builder builder(String directive) {
    return new UsageDefinition.Builder(directive);
  }

  public static final class Builder {
    private String directive;
    private final List<TokenDefinition> tokens;
    private int currentOrdinal;

    public Builder(String directive) {
      this.directive = directive;
      this.currentOrdinal = 0;
      this.tokens = new ArrayList<>();
    }

    public void addToken(String name, TokenType type) {
      TokenDefinition spec = new TokenDefinition(name, type, currentOrdinal);
      currentOrdinal++;
      tokens.add(spec);
    }

    public void addToken(String name, TokenType type, boolean optional) {
      TokenDefinition spec = new TokenDefinition(name, type, currentOrdinal, optional);
      currentOrdinal++;
      tokens.add(spec);
    }

    public void addToken(String name, TokenType type, int ordinal) {
      TokenDefinition spec = new TokenDefinition(name, type, ordinal, false);
      tokens.add(spec);
    }

    public void addToken(String name, TokenType type, boolean optional, int ordinal) {
      TokenDefinition spec = new TokenDefinition(name, type, ordinal, optional);
      tokens.add(spec);
    }

    public UsageDefinition build() {
      return new UsageDefinition(directive, tokens);
    }
  }

  public JsonObject toJsonObject() {
    JsonObject object = new JsonObject();
    object.addProperty("directive", directive);
    JsonArray array = new JsonArray();
    for (TokenDefinition token : tokens) {
      array.add(token.toJsonObject());
    }
    object.add("tokens", array);
    return object;
  }
}
