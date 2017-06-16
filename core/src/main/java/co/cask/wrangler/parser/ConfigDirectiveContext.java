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

package co.cask.wrangler.parser;

import co.cask.wrangler.api.DirectiveConfig;
import co.cask.wrangler.api.DirectiveContext;
import com.google.gson.Gson;

/**
 * Class description here.
 */
public class ConfigDirectiveContext implements DirectiveContext {
  private DirectiveConfig config;

  public ConfigDirectiveContext(DirectiveConfig config) {
    this.config = config;
  }

  public ConfigDirectiveContext(String json) {
    Gson gson = new Gson();
    this.config = gson.fromJson(json, DirectiveConfig.class);
  }

  @Override
  public boolean hasAlias(String directive) {
    return config.hasAlias(directive);
  }

  @Override
  public String getAlias(String directive) {
    return config.getAlias(directive);
  }

  @Override
  public boolean isExcluded(String directive) {
    return config.isExcluded(directive);
  }
}
