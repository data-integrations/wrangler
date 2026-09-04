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
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * This class {@link DirectiveConfig} defines the configuration for the
 * Wrangler.
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
 *   },
 *   "jexlAllowlist" : [
 *     {
 *       "className": "java.lang.Runtime",
 *       "methods": ["*"],
 *       "properties": ["*"]
 *   }
 *   ]
 *  }
 */
@Deprecated
public final class DirectiveConfig {
  public static final DirectiveConfig EMPTY = new DirectiveConfig();
  private static final String EXCLUSIONS_KEY = "exclusions";
  private static final String ALIASES_KEY = "aliases";
  public static final String JEXL_ALLOWLIST_KEY = "jexlAllowlist";

  private final Set<String> exclusions;
  private final Map<String, String> aliases;
  private final List<JexlAllowlist> jexlAllowlist;

  public DirectiveConfig() {
    this(Collections.emptySet(), Collections.emptyMap(), Collections.emptyList());
  }

  public DirectiveConfig(@Nullable Set<String> exclusions,
                         @Nullable Map<String, String> aliases,
                         @Nullable List<JexlAllowlist> jexlAllowlist) {
    this.exclusions = exclusions == null ? new HashSet<>() : new HashSet<>(exclusions);
    this.aliases = aliases == null ? new HashMap<>() : new HashMap<>(aliases);
    this.jexlAllowlist = jexlAllowlist == null ? null : new ArrayList<>(jexlAllowlist);
  }

  /**
   * Custom GSON adapter for {@link DirectiveConfig}.
   */
  public static final class DirectiveConfigDeserializer implements JsonDeserializer<DirectiveConfig> {

    @Override
    public DirectiveConfig deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {

      JsonObject jsonObject = json.getAsJsonObject();

      Set<String> exclusions = Collections.emptySet();
      if (jsonObject.has(EXCLUSIONS_KEY) && !jsonObject.get(EXCLUSIONS_KEY).isJsonNull()) {
        exclusions = context.deserialize(jsonObject.get(EXCLUSIONS_KEY), 
                                         new TypeToken<HashSet<String>>() { }.getType());
      }

      Map<String, String> aliases = Collections.emptyMap();
      if (jsonObject.has(ALIASES_KEY) && !jsonObject.get(ALIASES_KEY).isJsonNull()) {
        aliases = context.deserialize(jsonObject.get(ALIASES_KEY), 
                                      new TypeToken<HashMap<String, String>>() { }.getType());
      }

      List<JexlAllowlist> jexlAllowlist = null;
      if (jsonObject.has(JEXL_ALLOWLIST_KEY) && !jsonObject.get(JEXL_ALLOWLIST_KEY).isJsonNull()) {
        jexlAllowlist = context.deserialize(jsonObject.get(JEXL_ALLOWLIST_KEY), 
                                            new TypeToken<List<JexlAllowlist>>() { }.getType());
      }

      return new DirectiveConfig(exclusions, aliases, jexlAllowlist);
    }
  }

  /**
   * Gets the list of JEXL inclusions.
   *
   * @return the list of JEXL inclusions
   */
  @Nullable
  public List<JexlAllowlist> getJexlAllowlist() {
    return jexlAllowlist == null ? null : Collections.unmodifiableList(jexlAllowlist);
  }

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
    object.add(EXCLUSIONS_KEY, gson.toJsonTree(exclusions));
    object.add(ALIASES_KEY, gson.toJsonTree(aliases));
    object.add(JEXL_ALLOWLIST_KEY, gson.toJsonTree(jexlAllowlist));
    return object;
  }
}
