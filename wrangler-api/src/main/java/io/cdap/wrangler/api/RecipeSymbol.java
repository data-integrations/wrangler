/*
 *  Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.annotations.PublicEvolving;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.Token;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * RecipeSymbol holds the parsed tokens for a recipe.
 */

@PublicEvolving
public final class RecipeSymbol {
  private final String version;
  private final Set<String> loadableDirectives;
  private final List<TokenGroup> tokens;

  private RecipeSymbol(String version, Set<String> loadableDirectives, List<TokenGroup> tokens) {
    this.version = version;
    this.loadableDirectives = loadableDirectives;
    this.tokens = tokens;
  }

  public Set<String> getLoadableDirectives() {
    return loadableDirectives;
  }

  public String getVersion() {
    return version;
  }

  public int size() {
    return tokens.size();
  }

  public Iterator<TokenGroup> iterator() {
    return tokens.iterator();
  }

  public static RecipeSymbol.Builder builder() {
    return new RecipeSymbol.Builder();
  }

  public JsonElement toJson() {
    JsonObject output = new JsonObject();
    output.addProperty("class", this.getClass().getSimpleName());
    output.addProperty("count", tokens.size());
    JsonArray array = new JsonArray();
    for (TokenGroup token : tokens) {
      JsonArray darray = new JsonArray();
      Iterator<Token> it = token.iterator();
      while (it.hasNext()) {
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

  /**
   * Builder class for RecipeSymbol.
   * Helps in constructing RecipeSymbol instances by accumulating tokens and
   * metadata.
   */

  public static final class Builder {
    private final List<TokenGroup> groups = new ArrayList<>();
    private final Set<String> loadableDirectives = new TreeSet<>();
    private TokenGroup group = null;
    private String version = "1.0";

    public void createTokenGroup(SourceInfo info) {
      if (group != null) {
        groups.add(group);
      }
      this.group = new TokenGroup(info);
    }

    public void addToken(Token token) {
      group.add(token);
    }

    public void addToken(TimeDuration token) {
      group.add((Token) token); // ✅ Cast to Token explicitly
    }

    public void addVersion(String version) {
      this.version = version;
    }

    public void addLoadableDirective(String directive) {
      loadableDirectives.add(directive);
    }

    public RecipeSymbol build() {
      groups.add(group);
      return new RecipeSymbol(version, loadableDirectives, this.groups);
    }
  }
}
