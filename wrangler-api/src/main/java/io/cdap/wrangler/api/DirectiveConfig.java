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

package io.cdap.wrangler.api;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class {@link DirectiveConfig} defines the configuration for the Wrangler.
 * It specifies the directive exclusions -- meaning directives that should
 * not be accessible to the users and as well as directive aliases.
 *
 * {
 *   "exclusions" : [
 *      "parse-as-csv",
 *      "parse-as-excel",
 *      "set",
 *      "invoke-http"
 *    ],
 *  "aliases" : {
 *      "json-parser" : "parse-as-json",
 *      "js-parser" : "parse-as-json"
 *   }
 *  }
 */
@Deprecated
public final class DirectiveConfig {
  public static final DirectiveConfig EMPTY = new DirectiveConfig();
  // RecipeParser to be excluded or made non-accessible.
  private final Set<String> exclusions = new HashSet<>();

  // RecipeParser to be aliased.
  private final Map<String, String> aliases = new HashMap<>();


  /**
   * Checks if a directive is aliased.
   *
   * @param directive to checked for alias.
   * @return
   */
  public boolean hasAlias(String directive) {
    return aliases.containsKey(directive);
  }

  /**
   * Dereferences an alias if defined, else returns the directive itself.
   *
   * @param directive to be dereferenced.
   * @return dereferenced directive or null.
   */
  public String getAliasName(String directive) {
    return aliases.get(directive);
  }

  public Map<String, List<String>> getReverseAlias() {
    Map<String, List<String>> reverse = new HashMap<>();
    for (Map.Entry<String, String> alias : aliases.entrySet()) {
      List<String> list;
      if (reverse.containsKey(alias.getValue())) {
        list = reverse.get(alias.getValue());
      } else {
        list = new ArrayList<>();
      }
      list.add(alias.getKey());
      reverse.put(alias.getValue(), list);
    }
    return reverse;
  }

  /**
   * Checks if the directive should be excluded.
   *
   * @param directive to checked if it has to be excluded.
   * @return true if directive is excluded, false otherwise.
   */
  public boolean isExcluded(String directive) {
    return exclusions.contains(directive);
  }

  /**
   * Converts this object into a {@link JsonElement}.
   *
   * @return {@link JsonElement} representation of this object.
   */
  public JsonElement toJson() {
    Gson gson = new Gson();
    JsonObject object = new JsonObject();
    object.add("exclusions", gson.toJsonTree(exclusions));
    object.add("aliases", gson.toJsonTree(aliases));
    return object;
  }
}
