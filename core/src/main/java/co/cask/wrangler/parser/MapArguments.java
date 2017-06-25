/*
 *  Copyright © 2017 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.wrangler.parser;

import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.parser.Token;
import co.cask.wrangler.api.parser.TokenDefinition;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Class description here.
 */
public class MapArguments implements Arguments {
  private final Map<String, Token> tokens;

  public MapArguments(UsageDefinition definition, TokenGroup group) throws DirectiveParseException {
    this.tokens = new HashMap<>();

    List<TokenDefinition> specifications = definition.getTokens();
    Iterator<Token> it = group.iterator();
    int pos = 0;
    it.next(); // skip directive name.
    while (it.hasNext()) {
      Token token = it.next();
      while(pos < specifications.size()) {
        TokenDefinition specification = specifications.get(pos);
        if (!specification.optional()) {
          if (!specification.type().equals(token.type())) {
            throw new DirectiveParseException(
              String.format("Expected argument '%s' to be of type '%s', but it is of type '%s'",
                            specification.name(), specification.type().name(),
                            token.type().name())
            );
          } else {
            tokens.put(specification.name(), token);
            pos++;
            break;
          }
        } else {
          pos = pos + 1;
          if (specification.type().equals(token.type())) {
            tokens.put(specification.name(), token);
            break;
          }
        }
      }
    }
  }

  @Override
  public int size() {
    return tokens.size();
  }

  @Override
  public boolean contains(String name) {
    return tokens.containsKey(name);
  }

  @Override
  public <T> T value(String name) {
    return (T) tokens.get(name);
  }

  @Override
  public TokenType type(String name) {
    return tokens.get(name).type();
  }

  @Override
  public JsonElement toJsonObject() {
    JsonObject object = new JsonObject();
    for(Map.Entry<String, Token> entry : tokens.entrySet()) {
      object.add(entry.getKey(), entry.getValue().toJsonObject());
    }
    return object;
  }
}
