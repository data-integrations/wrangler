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

/**
 * Defines a token's name, type and whether it's optional.
 */
public class TokenDefinition {
  private final String name;
  private final TokenType type;
  private final boolean optional;

  public TokenDefinition(String name, TokenType type) {
    this(name, type, false);
  }

  public TokenDefinition(String name, TokenType type, boolean optional) {
    this.name = name;
    this.type = type;
    this.optional = optional;
  }

  public String getName() {
    return name;
  }

  public TokenType getType() {
    return type;
  }

  public boolean isOptional() {
    return optional;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    
    switch (type) {
      case COLUMN_NAME:
        if (optional) {
          builder.append("[:");
          builder.append(name);
          builder.append("]");
        } else {
          builder.append(":");
          builder.append(name);
        }
        break;
      case COLUMN_NAME_LIST:
        builder.append(":");
        builder.append(name);
        builder.append(" [,:");
        builder.append(name);
        builder.append("  ]*");
        break;
      case EXPRESSION:
        builder.append("exp:{<");
        builder.append(name);
        builder.append(">}");
        if (optional) {
          builder.insert(0, "[");
          builder.append("]");
        }
        break;
      case TEXT:
        if (optional) {
          builder.append("[' ");  // Add space after quote
          builder.append(name);
          builder.append("']");
        } else {
          builder.append("'");
          builder.append(name);
          builder.append("'");
        }
        break;
      case BOOLEAN:
        if (optional) {
          builder.append("[");
          builder.append(name);
          builder.append(" (true/false)]");
        } else {
          builder.append(name);
          builder.append(" (true/false)");
        }
        break;
      default:
        builder.append(name);
    }
    
    return builder.toString();
  }
}
