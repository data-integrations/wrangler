/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.proto;

import java.util.List;
import java.util.Objects;

/**
 * Defines the recipe object that is part of the {@link Request}
 */
public class Recipe {
  // RecipeParser specified by the user
  private final List<String> directives;

  // Ability to save the directives
  private final Boolean save;

  // Name of the recipe.
  private final String name;

  public Recipe(List<String> directives, Boolean save, String name) {
    this.directives = directives;
    this.save = save;
    this.name = name;
  }

  /**
   * @return List of directives specified by the user.
   */
  public List<String> getDirectives() {
    return directives;
  }

  /**
   * Adds a pragma directive at the front of the recipe.
   */
  public void setPragma(String directive) {
    if (directive != null) {
      String pragma = directives.size() > 0 ? directives.get(0) : null;
      if (pragma != null && !pragma.matches("(.*)load-directives(.*)")) {
        directives.add(0, directive);
      } else {
        directives.set(0, directive);
      }
    }
  }

  /**
   * @return true if directives need to be saved, else false.
   */
  public Boolean getSave() {
    return save;
  }

  /**
   * @return name of the recipe.
   */
  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Recipe recipe = (Recipe) o;
    return Objects.equals(directives, recipe.directives) &&
      Objects.equals(save, recipe.save) &&
      Objects.equals(name, recipe.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(directives, save, name);
  }
}
