/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.wrangler.proto.recipe;

import co.cask.cdap.internal.guava.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Stores information about a recipe in the backend dataset.
 */
public final class RecipeDatum {
  private static final Gson GSON = new GsonBuilder().create();
  // Id of the recipe.
  private String id;

  // Unix timestamp about when the recipe was created.
  private Long created;

  // Unix timestamp about when the recipe was last updated.
  private Long updated;

  // List of directives that make-up recipe.
  private String directives;

  // Name of the recipe - display name.
  private String name;

  // Description for the recipe.
  private String description;

  /**
   * Create information about a recipe. Timestamps are in seconds.
   */
  public RecipeDatum(String id, String name, String description, long created, long updated, List<String> directives) {
    this.id = id;
    this.name = name;
    this.description = description;
    this.created = created;
    this.updated = updated;
    this.directives = GSON.toJson(directives);
  }

  /**
   * @return recipe id.
   */
  public String getId() {
    return id;
  }

  /**
   * @return Unix timestamp about when the recipe was created.
   */
  public long getCreated() {
    return created;
  }

  /**
   * @return Unix timestamp about when the recipe was updated.
   */
  public long getUpdated() {
    return updated;
  }

  /**
   * Sets the time when the recipe was updated.
   * @param updated unix timestamp of when recipe was updated.
   */
  public void setUpdated(Long updated) {
    this.updated = updated;
  }

  /**
   * @return List of directives that is part of recipe.
   */
  public List<String> getDirectives() {
    if (directives != null) {
      return GSON.fromJson(directives, new TypeToken<List<String>>() { }.getType());
    } else {
      return new ArrayList<>();
    }
  }

  /**
   * @return Display name of the directive.
   */
  public String getName() {
    return name;
  }

  /**
   * @return Description associated with the directive.
   */
  public String getDescription() {
    return description;
  }

}
