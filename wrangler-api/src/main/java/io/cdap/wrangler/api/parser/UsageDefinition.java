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

import java.util.ArrayList;
import java.util.List;

/**
 * Defines the usage pattern for a directive.
 */
public class UsageDefinition {
  private final String directiveName;
  private String description;
  private List<TokenDefinition> tokens;
  
  private UsageDefinition(String name) {
    this.directiveName = name;
    this.tokens = new ArrayList<>();
  }
  
  public String getDirectiveName() {
    return directiveName;
  }
  
  public String getDescription() {
    return description;
  }
  
  public List<TokenDefinition> getTokens() {
    return tokens;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(directiveName);
    
    for (int i = 0; i < tokens.size(); i++) {
      builder.append(" ");
      builder.append(tokens.get(i).toString());
    }
    
    return builder.toString();
  }
  
  public static Builder builder(String directiveName) {
    return new Builder(directiveName);
  }
  
  /**
   * Builder class for constructing UsageDefinition instances.
   */
  public static class Builder {
    private final UsageDefinition definition;
    
    public Builder(String directiveName) {
      this.definition = new UsageDefinition(directiveName);
    }
    
    public Builder setDescription(String description) {
      this.definition.description = description;
      return this;
    }
    
    public Builder define(String name, TokenType type) {
      this.definition.tokens.add(new TokenDefinition(name, type));
      return this;
    }

    public Builder define(String name, TokenType type, boolean optional) {
      this.definition.tokens.add(new TokenDefinition(name, type, optional));
      return this;
    }
    
    public UsageDefinition build() {
      return definition;
    }
  }
}
