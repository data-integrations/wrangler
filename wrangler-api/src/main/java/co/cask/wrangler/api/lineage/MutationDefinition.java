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

package co.cask.wrangler.api.lineage;

import co.cask.wrangler.api.annotations.PublicEvolving;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The <code>MutationDefinition</code> class represents a definition of token as specified
 * by the user while defining a directive usage. All definitions of a token are represented
 * by a instance of this class.
 *
 * The definition are constant (immutable) and they cannot be changed once defined.
 * For example :
 * <code>
 *   MutationDefinition token = new MutationDefinition("col1", MutationType.READ);
 * </code>
 *
 * <p>The class <code>TokenDefinition</code> includes methods for retrieving different members of
 * like name of the token, type of the token, label associated with token, whether it's optional or not
 * and the ordinal number of the token in the <code>TokenGroup</code>.</p>
 *
 * <p>As this class is immutable, the constructor requires all the member variables to be presented
 * for an instance of this object to be created.</p>
 */
@PublicEvolving
public final class MutationDefinition implements Serializable {
  private final String directive;
  private final String description; // Optional
  private final List<Mutation> mutations;

  private MutationDefinition(String directive, String description, List<Mutation> mutations) {
    this.directive = directive;
    this.description = description;
    this.mutations = mutations;
  }

  /**
   * @return name of the directive.
   */
  public String directive() {
    return directive;
  }

  /**
   * @return description of the directive.
   */
  public String description() {
    return description;
  }

  /**
   * @return Returns the iterator for all the <code>Mutations</code>.
   */
  public Iterator<Mutation> iterator() {
    return mutations.iterator();
  }

  /**
   * Returns the <code>JsonElement</code> of this object <code>MutationDefinition</code>.
   * @return An instance of <code>JsonElement</code> representing <code>MutationDefinition</code>
   * this object.
   */
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("directive", directive);
    object.addProperty("description", description);
    JsonArray array = new JsonArray();
    for (Mutation mutation : mutations) {
      array.add(mutation.toJson());
    }
    object.add("mutations", array);
    return object;
  }

  public static MutationDefinition.Builder builder(String directive) {
    return new MutationDefinition.Builder(directive);
  }

  /**
   * Builder for {@link MutationDefinition}.
   */
  public static class Builder {
    private final String directive;
    private final String description;
    private final List<Mutation> mutations;

    public Builder(String directive) {
      this(directive, null);
    }

    public Builder(String directive, String description) {
      this.directive = directive;
      this.description = description;
      this.mutations = new ArrayList<>();
    }

    public void addMutation(String column, MutationType type) {
      mutations.add(new Mutation(column, type));
    }

    public MutationDefinition build() {
      return new MutationDefinition(directive, description, mutations);
    }
  }
}
