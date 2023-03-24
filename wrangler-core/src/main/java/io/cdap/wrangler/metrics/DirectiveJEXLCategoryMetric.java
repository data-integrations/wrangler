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

import com.google.common.collect.ImmutableMap;
import io.cdap.wrangler.api.EntityMetricDef;
import io.cdap.wrangler.api.EntityMetrics;
import io.cdap.wrangler.expression.EL;

import java.util.Map;

import static io.cdap.cdap.api.metrics.MetricType.COUNTER;
import static io.cdap.cdap.common.conf.Constants.Metrics.Tag.APP_ENTITY_TYPE;
import static io.cdap.cdap.common.conf.Constants.Metrics.Tag.APP_ENTITY_TYPE_NAME;
import static io.cdap.cdap.common.conf.Constants.Metrics.Tag.SERVICE;

/**
 * Represents a directive that emits the 'JEXL category' metric. This metric counts the number of times
 * a JEXL function category in {@link io.cdap.wrangler.expression.EL.DefaultFunctions} is used.
 */
public interface DirectiveJEXLCategoryMetric extends EntityMetrics {
  String JEXL_CATEGORY_METRIC_NAME = "wrangler.jexl-category.count";
  int JEXL_CATEGORY_METRIC_COUNT = 1;
  String JEXL_CATEGORY_ENTITY_NAME = "jexl-category";
  String APPLICATION_NAME = "dataprep";
  Map<?, ?> EL_DEFAULT_FUNCTIONS = new EL.DefaultFunctions().functions();

  /**
   * This method parses the JEXL function category from the given JEXL script and returns the metric if the category is
   * present in {@link io.cdap.wrangler.expression.EL.DefaultFunctions}
   *
   * @param jexlScript the JEXL script from which the JEXL function category will be parsed
   * @return {@link EntityMetricDef} with the metric name and necessary tags representing a JEXL category metric
   */
  default EntityMetricDef getJEXLCategoryMetric(String jexlScript) {
    String jexlCategory = jexlScript.split(":")[0].trim();
    if (EL_DEFAULT_FUNCTIONS.containsKey(jexlCategory)) {
      return new EntityMetricDef(
        JEXL_CATEGORY_METRIC_NAME, ImmutableMap.of(SERVICE, APPLICATION_NAME,
                                                   APP_ENTITY_TYPE, JEXL_CATEGORY_ENTITY_NAME,
                                                   APP_ENTITY_TYPE_NAME, jexlCategory),
        COUNTER, JEXL_CATEGORY_METRIC_COUNT);
    }
    return null;
  }
}
