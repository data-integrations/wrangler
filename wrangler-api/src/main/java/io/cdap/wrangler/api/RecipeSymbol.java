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
import io.cdap.wrangler.api.parser.Token;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * This object <code>RecipeSymbol</code> stores information about all the
 * <code>TokenGroup</code> ( TokenGroup represents a collection of tokens
 * generated from parsing a single directive). The object also contains
 * information about the directives (or plugins) that need to be loaded
 * at the startup time.
 *
 * <p>This class provides some useful methods for accessing the list of
 * directives or plugins that need to be loaded, the token groups for
 * all the directives tokenized and parsed.</p>
 *
 * <p>This class exposes a builder pattern for constructing the object.
 * in the <code>RecipeVisitor</code>. The <code>RecipeVisitor</code>
 * constructs <code>RecipeSymbol</code> using the <code>RecipeSymbol.Builder</code></p>
 */
@PublicEvolving
public final class RecipeSymbol {
  /**
   * Version if specified, else defaults to 1.0
   */
  private final String version;

  /**
   * Set of directives or plugins that have to loaded
   * during the configuration phase of <code>RecipePipeline.</code>
   */
  private final Set<String> loadableDirectives;

  /**
   * This maintains a list of tokens for each directive parsed.
   */
  private final List<TokenGroup> tokens;

  private RecipeSymbol(String version, Set<String> loadableDirectives, List<TokenGroup> tokens) {
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
   * Returns the version of the grammar as specified in the recipe. The
   * version is the one extracted from Pragma. It's specified as follows
   * <code>#pragma version 2.0;</code>
   *
   * @return version of the grammar used in the recipe.
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
   * Static method for creating an instance of the {@code RecipeSymbol.Builder}.
   *
   * @return a instance of builder.
   */
  public static RecipeSymbol.Builder builder() {
    return new RecipeSymbol.Builder();
  }

  /**
   * This method <code>toJson</code> returns the <code>JsonElement</code> object
   * representation of this object.
   *
   * @return An instance of <code>JsonElement</code> representing this object.
   */
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
   * This inner class provides a builder pattern for building
   * the <code>RecipeSymbol</code> object. In order to create the
   * this builder, one has to use the static method defined in
   * <code>RecipeSymbol</code>.
   *
   * Following is an example of how this can be done.
   *
   * <code>
   *   RecipeSymbol.Builder builder = RecipeSymbol.builder();
   *   builder.createTokenGroup(...);
   *   builder.addToken(...);
   *   builder.addVersion(...);
   *   builder.addLoadableDirective(...);
   *   RecipeSymbol compiled = builder.build();
   * </code>
   */
  public static final class Builder {
    private final List<TokenGroup> groups = new ArrayList<>();
    private final Set<String> loadableDirectives = new TreeSet<>();
    private TokenGroup group = null;
    private String version = "1.0";

    /**
     * <code>TokenGroup</code> is created for each directive in
     * the recipe. This method creates a new <code>TokenGroup</code>
     * by passing the <code>SourceInfo</code>, which represents the
     * information of the source parsed.
     *
     * @param info about the source directive being parsed.
     */
    public void createTokenGroup(SourceInfo info) {
      if (group != null) {
        groups.add(group);
      }
      this.group = new TokenGroup(info);
    }

    /**
     * This method provides a way to add a <code>Token</code> to the <code>TokenGroup</code>.
     *
     * @param token to be added to the token group.
     */
    public void addToken(Token token) {
      group.add(token);
    }

    /**
     * Recipe can specify the version of the grammar. This method
     * allows one to extract and add the version to the <code>RecipeSymbol.</code>
     *
     * @param version of the recipe grammar being used.
     */
    public void addVersion(String version) {
      this.version = version;
    }

    /**
     * A Recipe can specify the pragma instructions for loading the directives
     * dynamically. This method allows adding the new directive to be loaded
     * as it's parsing through the call graph.
     *
     * @param directive to be loaded dynamically.
     */
    public void addLoadableDirective(String directive) {
      loadableDirectives.add(directive);
    }

    /**
     * Returns a fully constructed and valid <code>RecipeSymbol</code> object.
     *
     * @return An instance of <code>RecipeSymbol</code>
     */
    public RecipeSymbol build() {
      groups.add(group);
      return new RecipeSymbol(version, loadableDirectives, this.groups);
    }
  }
}
