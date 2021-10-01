/*
 * Copyright © 2021 Cask Data, Inc.
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
package io.cdap.wrangler;

import io.cdap.cdap.api.annotation.RuntimeImplementation;
import io.cdap.cdap.etl.api.dl.DLDataSet;
import io.cdap.cdap.etl.api.dl.DLExpressionFactory;
import io.cdap.cdap.etl.api.dl.DLPluginContext;
import io.cdap.cdap.etl.api.dl.SimpleDLPluginRuntimeImplementation;
import io.cdap.cdap.etl.api.engine.sql.StandardSQLCapabilities;
import io.cdap.wrangler.Wrangler.Config;

/**
 * Wrangler implementation with BQ pushdown. Currently it's not implemented
 */
@RuntimeImplementation(pluginClass = Wrangler.class, order = Wrangler.ORDER_SQL)
public class SQLWrangler implements SimpleDLPluginRuntimeImplementation {

  private final Config config;

  public SQLWrangler(Config config) {
    this.config = config;
    if (!Config.EL_SQL.equalsIgnoreCase(config.getExpressionLanguage())) {
      //Fall back
      throw new IllegalStateException("This implementation runs for SQL language");
    }
    if (config.getDirectives() != null && !config.getDirectives().trim().isEmpty()) {
      throw new IllegalStateException("We only run this for empty directives list");
    }
  }

  @Override
  public void initialize(DLPluginContext context) throws Exception {
    if (!context.getDLContext().getExpressionFactory(StandardSQLCapabilities.SQL).isPresent()) {
      throw new IllegalStateException("Expression language does not implement SQL");
    }
  }

  @Override
  public DLDataSet transform(DLPluginContext context, DLDataSet input) {
    DLExpressionFactory expressionFactory = context.getDLContext()
        .getExpressionFactory(StandardSQLCapabilities.SQL).get();
    return input.filter(expressionFactory.compile("not (" + config.getPrecondition() + ")"));
  }
}
