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

package io.cdap.wrangler.api;

import io.cdap.wrangler.api.parser.UsageDefinition;
import java.util.Collections;
import java.util.List;

/**
 * Abstract base class for directives that provides common functionality.
 */
public abstract class AbstractDirective implements Directive {

  /** Metric name for directive usage tracking. */
  private static final String DIRECTIVE_METRIC_NAME = "directive.usage";
  
  /** Entity type for directives in metrics. */
  private static final String DIRECTIVE_ENTITY_TYPE = "directive";
  
  /** Default count value for directive metrics. */
  private static final long DIRECTIVE_METRIC_COUNT = 1L;

  @Override
  public List<EntityCountMetric> getCountMetrics() {
    UsageDefinition definition = define();
    if (definition != null) {
      return Collections.singletonList(new EntityCountMetric(
        DIRECTIVE_METRIC_NAME,
        DIRECTIVE_ENTITY_TYPE,
        definition.getDirectiveName(),
        DIRECTIVE_METRIC_COUNT));
    }
    return Collections.emptyList();
  }
}
