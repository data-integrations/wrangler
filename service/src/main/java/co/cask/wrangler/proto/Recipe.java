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

package co.cask.wrangler.proto;

import java.util.List;

/**
 * Defines the recipe object that is part of the {@link Request}
 */
public class Recipe {
  // Directives specified by the user
  private List<String> directives;

  // Ability to save the directives
  private Boolean save;

  // Name of the recipe.
  private String name;

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
}
