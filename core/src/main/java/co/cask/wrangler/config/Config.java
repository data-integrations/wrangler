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

package co.cask.wrangler.config;

import java.util.Map;
import java.util.Set;

/**
 * This class {@link Config} defines the configuration for the Wrangler.
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
public final class Config {
  // Directives to be excluded or made non-accessible.
  private Set<String> exclusions;

  // Directives to be aliased.
  private Map<String, String> aliases;

  /**
   * @return Set of directives that are excluded.
   */
  public Set<String> getExclusions() {
    return exclusions;
  }

  /**
   * @return Map of directives aliased.
   */
  public Map<String, String> getAliases() {
    return aliases;
  }

  /**
   * Checks if a directive is aliased.
   *
   * @param directive to checked for alias.
   * @return
   */
  public boolean isAliased(String directive) {
    if (aliases != null && aliases.containsKey(directive)) {
      return true;
    }
    return false;
  }

  /**
   * Dereferences an alias if defined, else returns the directive itself.
   *
   * @param directive to be dereferenced.
   * @return dereferenced directive or the directive itself.
   */
  public String dereferenceAlias(String directive) {
    if (isAliased(directive)) {
      return aliases.get(directive);
    }
    return directive;
  }

  /**
   * Checks if the directive should be excluded.
   *
   * @param directive to checked if it has to be excluded.
   * @return true if directive is excluded, false otherwise.
   */
  public boolean isExcluded(String directive) {
    if (exclusions != null && exclusions.contains(directive)) {
      return true;
    }
    return false;
  }

  /**
   * Checks if the directive should be included.
   *
   * @param directive to checked if it has to be included.
   * @return true if directive is included, false otherwise.
   */
  public boolean isIncluded(String directive) {
    return !isExcluded(directive);
  }
}
