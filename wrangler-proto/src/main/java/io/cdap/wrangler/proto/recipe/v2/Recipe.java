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

package io.cdap.wrangler.proto.recipe.v2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Metadata information about a Recipe with only fields necessary for API user
 */
public class Recipe {
  private final RecipeId recipeId;
  private final String recipeName;
  private final String description;
  private final List<String> directives;
  private final long createdTimeMillis;
  private final long updatedTimeMillis;
  private final int recipeStepsCount;

  private Recipe(RecipeId recipeId, String recipeName, String description, List<String> directives,
                 long createdTimeMillis, long updatedTimeMillis, int recipeStepsCount) {
    this.recipeName = recipeName;
    this.recipeId = recipeId;
    this.description = description;
    this.directives = Collections.unmodifiableList(directives);
    this.createdTimeMillis = createdTimeMillis;
    this.updatedTimeMillis = updatedTimeMillis;
    this.recipeStepsCount = recipeStepsCount;
  }

  public String getRecipeName() {
    return recipeName;
  }

  public RecipeId getRecipeId() {
    return recipeId;
  }

  public String getDescription() {
    return description;
  }

  public List<String> getDirectives() {
    return directives;
  }

  public long getCreatedTimeMillis() {
    return createdTimeMillis;
  }

  public long getUpdatedTimeMillis() {
    return updatedTimeMillis;
  }

  public int getRecipeStepsCount() {
    return recipeStepsCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Recipe other = (Recipe) o;
    return Objects.equals(recipeId, other.recipeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(recipeName, recipeId, directives);
  }

  public static Builder builder(RecipeId recipeId) {
    return new Builder(recipeId);
  }

  public static Builder builder(Recipe existing) {
    return new Builder(existing.getRecipeId())
      .setRecipeName(existing.getRecipeName())
      .setDescription(existing.getDescription())
      .setDirectives(existing.getDirectives())
      .setCreatedTimeMillis(existing.getCreatedTimeMillis())
      .setUpdatedTimeMillis(existing.getUpdatedTimeMillis())
      .setRecipeStepsCount(existing.getRecipeStepsCount());
  }

  /**
   * Creates a Recipe meta object
   */
  public static class Builder {
    private final RecipeId recipeId;
    private String recipeName;
    private String description;
    private List<String> directives;
    private long createdTimeMillis;
    private long updatedTimeMillis;
    private int recipeStepsCount;

    Builder(RecipeId recipeId) {
      this.recipeId = recipeId;
      this.directives = new ArrayList<>();
    }

    public Builder setRecipeName(String recipeName) {
      this.recipeName = recipeName;
      return this;
    }

    public Builder setDescription(String description) {
      this.description = description;
      return this;
    }

    public Builder setDirectives(List<String> directives) {
      this.directives.clear();
      this.directives.addAll(directives);
      return this;
    }

    public Builder setCreatedTimeMillis(long createdTimeMillis) {
      this.createdTimeMillis = createdTimeMillis;
      return this;
    }

    public Builder setUpdatedTimeMillis(long updatedTimeMillis) {
      this.updatedTimeMillis = updatedTimeMillis;
      return this;
    }

    public Builder setRecipeStepsCount(int recipeStepsCount) {
      this.recipeStepsCount = recipeStepsCount;
      return this;
    }

    public Recipe build() {
      return new Recipe(recipeId, recipeName, description, directives,
                        createdTimeMillis, updatedTimeMillis, recipeStepsCount);
    }
  }
}
