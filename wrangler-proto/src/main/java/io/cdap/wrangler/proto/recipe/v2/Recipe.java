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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Metadata information about a Recipe
 */
public class Recipe {
  private final String recipeName;
  private final String recipeId;
  private final List<String> directives;
  private final long createdTimeMillis;
  private final long updatedTimeMillis;

  private Recipe(String recipeName, String recipeId, List<String> directives,
                 long createdTimeMillis, long updatedTimeMillis) {
    this.recipeName = recipeName;
    this.recipeId = recipeId;
    this.directives = directives;
    this.createdTimeMillis = createdTimeMillis;
    this.updatedTimeMillis = updatedTimeMillis;
  }

  public String getRecipeName() {
    return recipeName;
  }

  public String getRecipeId() {
    return recipeId;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Recipe other = (Recipe) o;
    return Objects.equals(recipeId, other.recipeId) &&
      Objects.equals(recipeName, other.recipeName) &&
      Objects.equals(directives, other.directives);
  }

  @Override
  public int hashCode() {
    return Objects.hash(recipeName, recipeId, directives);
  }

  public static Builder builder(String recipeName, String recipeId) {
    return new Builder(recipeName, recipeId);
  }

  public static Builder builder(Recipe existing) {
    return new Builder(existing.getRecipeName(), existing.getRecipeId())
      .setDirectives(existing.getDirectives())
      .setCreatedTimeMillis(existing.getCreatedTimeMillis())
      .setUpdatedTimeMillis(existing.getUpdatedTimeMillis());
  }

  /**
   * Creates a Recipe meta object
   */
  public static class Builder {
    private final String recipeName;
    private final String recipeId;
    private final List<String> directives;
    private long createdTimeMillis;
    private long updatedTimeMillis;

    Builder(String recipeName, String recipeId) {
      this.recipeName = recipeName;
      this.recipeId = recipeId;
      this.directives = new ArrayList<>();
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

    public Recipe build() {
      return new Recipe(recipeName, recipeId, directives, createdTimeMillis, updatedTimeMillis);
    }
  }
}
