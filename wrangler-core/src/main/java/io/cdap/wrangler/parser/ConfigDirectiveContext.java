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

package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.DirectiveConfig;
import io.cdap.wrangler.api.DirectiveContext;

/**
 * This class {@link ConfigDirectiveContext} manages the context for directive
 * by either retrieving the configuration from a service or from a provided
 * instance of {@link DirectiveConfig}.
 */
public class ConfigDirectiveContext implements DirectiveContext {
  private final DirectiveConfig config;

  public ConfigDirectiveContext(DirectiveConfig config) {
    this.config = config;
  }

  /**
   * Checks if the directive is aliased.
   *
   * @param directive to be checked for aliasing.
   * @return true if the directive has an alias, false otherwise.
   */
  @Override
  public boolean hasAlias(String directive) {
    return config.hasAlias(directive);
  }

  /**
   * Returns the root directive aliasee
   * @param directive
   * @return
   */
  @Override
  public String getAlias(String directive) {
    return config.getAliasName(directive);
  }

  /**
   * Checks if the directive is being excluded from being used.
   *
   * @param directive to be checked for exclusion.
   * @return true if excluded, false otherwise.
   */
  @Override
  public boolean isExcluded(String directive) {
    return config.isExcluded(directive);
  }
}
