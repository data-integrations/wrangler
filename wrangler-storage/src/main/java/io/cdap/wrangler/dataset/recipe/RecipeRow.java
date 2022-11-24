/*
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
 */

package io.cdap.wrangler.dataset.recipe;

import io.cdap.wrangler.proto.recipe.v2.Recipe;
import java.util.Objects;

/**
 * Stores information about Recipe, including information that should not be exposed to users.
 * {@link Recipe} contains fields that are exposed to users.
 */
public class RecipeRow {
  private final Recipe recipe;

  private RecipeRow(Recipe recipe) {
    this.recipe = recipe;
  }

  public Recipe getRecipe() {
    return recipe;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RecipeRow other = (RecipeRow) o;
    return Objects.equals(recipe, other.recipe);
  }

  @Override
  public int hashCode() {
    return Objects.hash(recipe);
  }

  public static Builder builder(Recipe recipe) {
    return new Builder(recipe);
  }

  public static Builder builder(RecipeRow existing) {
    return new Builder(existing.getRecipe());
  }

  /**
   * Creates a RecipeRow storage object
   */
  public static class Builder {
    private final Recipe recipe;

    Builder(Recipe recipe) {
      this.recipe = recipe;
    }

    public RecipeRow build() {
      return new RecipeRow(recipe);
    }
  }
}
