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

import java.util.List;

/**
 * Same as {@link RecipeDatum} except directives are stored as a list of strings.
 *
 * TODO: consolidate the classes when revamping backend storage
 */
public class RecipeInfo {
  private final String id;
  private final String name;
  private final String description;
  private final long created;
  private final long updated;
  private final int length;
  private final List<String> directives;

  public RecipeInfo(String id, String name, String description, long created, long updated,
                    List<String> directives) {
    this.id = id;
    this.name = name;
    this.description = description;
    this.created = created;
    this.updated = updated;
    this.directives = directives;
    this.length = directives.size();
  }
}
