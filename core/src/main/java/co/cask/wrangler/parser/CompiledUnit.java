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

import co.cask.wrangler.api.annotations.PublicEvolving;
import co.cask.wrangler.api.parser.Token;
import co.cask.wrangler.api.parser.TokenType;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
@PublicEvolving
final class CompiledUnit {
  // This maintains a list of tokens for all the directives
  // being parsed.
  private final List<TokenGroup> tokens;

  private CompiledUnit(List<TokenGroup> tokens) {
    this.tokens = tokens;
  }

  public int size() {
    return tokens.size();
  }

  public Iterator<TokenGroup> iterator() {
    return tokens.iterator();
  }

  public static CompiledUnit.Builder builder() {
    return new CompiledUnit.Builder();
  }

  public JsonElement toJsonObject() {
    JsonObject output = new JsonObject();
    output.addProperty("class", this.getClass().getSimpleName());
    output.addProperty("count", tokens.size());
    JsonArray array = new JsonArray();
    Iterator<TokenGroup> iterator = tokens.iterator();
    while(iterator.hasNext()) {
      JsonArray darray = new JsonArray();
      Iterator<Token> it = iterator.next().iterator();
      while(it.hasNext()) {
        Token tok = it.next();
        JsonObject object = new JsonObject();
        object.addProperty("token", tok.type().toString());
        object.addProperty("value", tok.value().toString());
        darray.add(object);
      }
      array.add(darray);
    }
    output.add("value", array);
    return output;
  }

  public static final class Builder {
    private final List<TokenGroup> allTokens = new ArrayList<>();
    private TokenGroup tokens = new TokenGroup();

    public void add(Token token) {
      if (token.type() == TokenType.DIRECTIVE_NAME
        || token.type() == TokenType.UUD_DIRECTIVE) {
        if (allTokens.size() >= 0 && tokens.size() > 0) {
          allTokens.add(tokens);
          tokens = new TokenGroup();
        }
      }
      tokens.add(token);
    }

    public CompiledUnit build() {
      allTokens.add(tokens);
      return new CompiledUnit(this.allTokens);
    }
  }
}
