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

package co.cask.wrangler.api;

import co.cask.wrangler.api.annotations.PublicEvolving;
import co.cask.wrangler.api.parser.Token;
import co.cask.wrangler.api.parser.TokenType;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * This object <code>CompiledUnit</code> stored information about all the
 * <code>TokenGroup</code> ( TokenGroup represents a collection of tokens
 * generated from parsing a single directive). The object also contains
 * information about the directives (or plugins) that need to be loaded
 * at the startup time.
 *
 * <p>This class provides some useful methods for accessing the list of
 * directives or plugins that need to be loaded, the token groups for
 * all the directives tokenized and parsed.</p>
 *
 * <p>This class exposes a builder pattern for constructing the object.</p>
 */
@PublicEvolving
public final class CompiledUnit {
  /**
   * Version if specified, else defaults to 1.0
   */
  private String version;

  /**
   * Set of directives or plugins that have to loaded
   * during the configuration phase of <code>RecipePipeline.</code>
   */
  private Set<String> loadableDirectives = new HashSet<>();

  /**
   * This maintains a list of tokens for each directive parsed.
   */
  private final List<TokenGroup> tokens;

  private CompiledUnit(String version, Set<String> loadableDirectives, List<TokenGroup> tokens) {
    this.version = version;
    this.loadableDirectives = loadableDirectives;
    this.tokens = tokens;
  }

  /**
   * Returns a set of dynamically loaded directives as plugins. These are
   * the set of plugins or directives that are in the recipe, but are provided
   * as the user plugins.
   *
   * <p>If there are no directives specified in the recipe, then there would
   * be no plugins to be loaded.</p>
   *
   * @return An empty set if there are not directives to be loaded dynamically,
   * else the list of directives as specified in the recipe.
   */
  public Set<String> getLoadableDirectives() {
    return loadableDirectives;
  }

  /**
   * @return
   */
  public String getVersion() {
    return version;
  }

  /**
   * Returns number of groups tokenized and parsed. The number returned will
   * less than or equal to the number of directives specified in the recipe.
   *
   * <p>Fewer than number of directives is because of the '#pragma' directives</p>
   * @return
   */
  public int size() {
    return tokens.size();
  }

  /**
   * Returns an iterator to the list of token groups maintained by this object.
   *
   * @return iterator to the list of tokens maintained.
   */
  public Iterator<TokenGroup> iterator() {
    return tokens.iterator();
  }

  /**
   * Static method for creating an instance of the {@code CompiledUnit.Builder}.
   *
   * @return a instance of builder.
   */
  public static CompiledUnit.Builder builder() {
    return new CompiledUnit.Builder();
  }

  /**
   *
   * @return
   */
  public JsonElement toJson() {
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
    private TokenGroup tokens = null;
    private Set<String> loadableDirectives = new HashSet<>();
    private String version = "1.0";
    private SourceInfo info = null;

    public void add(SourceInfo info, Token token) {
      if (tokens == null) {
        tokens = new TokenGroup(info);
      }
      if (token.type() == TokenType.DIRECTIVE_NAME) {
        if (allTokens.size() >= 0 && tokens.size() > 0) {
          allTokens.add(tokens);
          tokens = new TokenGroup(info);
        }
      }
      tokens.add(token);
    }

    public void addVersion(String version) {
      this.version = version;
    }

    public void addLoadableDirective(String directive) {
      loadableDirectives.add(directive);
    }

    public CompiledUnit build() {
      allTokens.add(tokens);
      return new CompiledUnit(version, loadableDirectives, this.allTokens);
    }
  }
}
