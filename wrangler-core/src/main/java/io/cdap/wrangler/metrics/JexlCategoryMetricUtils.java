/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.wrangler.metrics;

import io.cdap.wrangler.api.EntityCountMetric;
import io.cdap.wrangler.expression.EL;
import org.apache.commons.jexl3.parser.ParserTokenManager;
import org.apache.commons.jexl3.parser.SimpleCharStream;

import java.io.ByteArrayInputStream;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Utility class for the 'JEXL category' metric. This metric counts the number of times a JEXL function category
 * in {@link io.cdap.wrangler.expression.EL.DefaultFunctions} is used.
 */
public final class JexlCategoryMetricUtils {
  private static final Map<?, ?> EL_DEFAULT_FUNCTIONS = new EL.DefaultFunctions().functions();

  // JEXL Metric constants
  public static final String JEXL_CATEGORY_METRIC_NAME = "wrangler.jexl-category.count";
  public static final int JEXL_CATEGORY_METRIC_COUNT = 1;
  public  static final String JEXL_CATEGORY_ENTITY_TYPE = "jexl-category";

  /**
   * This method parses the JEXL function category from the given JEXL script and returns the metric if the category is
   * present in {@link io.cdap.wrangler.expression.EL.DefaultFunctions}
   *
   * @param jexlScript the JEXL script from which the JEXL function category will be parsed
   * @return {@link EntityCountMetric} with the metric name and necessary tags representing a JEXL category metric
   */
  @Nullable
  public static EntityCountMetric getJexlCategoryMetric(String jexlScript) {
    String category = parseJexlCategory(jexlScript);
    if (EL_DEFAULT_FUNCTIONS.containsKey(category)) {
      return new EntityCountMetric(
        JEXL_CATEGORY_METRIC_NAME, JEXL_CATEGORY_ENTITY_TYPE, category, JEXL_CATEGORY_METRIC_COUNT);
    }
    return null;
  }

  private static String parseJexlCategory(String script) {
    ParserTokenManager manager = new ParserTokenManager(
      new SimpleCharStream(new ByteArrayInputStream(script.getBytes())));
    return manager.getNextToken().toString();
  }
}
