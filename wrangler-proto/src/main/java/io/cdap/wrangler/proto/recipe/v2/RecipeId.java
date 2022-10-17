/**
 * Copyright © 2022 Cask Data, Inc.
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
 *
 */
package io.cdap.wrangler.proto.recipe.v2;

import io.cdap.cdap.api.NamespaceSummary;

import java.util.Objects;
import java.util.UUID;

/**
 * Recipe id
 */
public class RecipeId {
  private final NamespaceSummary namespace;
  private final String recipeId;

  public RecipeId(NamespaceSummary namespace) {
    this(namespace, UUID.randomUUID().toString());
  }

  public RecipeId(NamespaceSummary namespace, String recipeId) {
    this.namespace = namespace;
    this.recipeId = recipeId;
  }

  public NamespaceSummary getNamespace() {
    return namespace;
  }

  public String getRecipeId() {
    return recipeId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RecipeId other = (RecipeId) o;
    return Objects.equals(namespace, other.namespace) &&
      Objects.equals(recipeId, other.recipeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, recipeId);
  }
}
