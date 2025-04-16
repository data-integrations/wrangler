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

import java.util.List;

/**
 * Metrics emitted by a Wrangler entity
 */
public interface EntityMetrics {
  /**
   * This method is used to return a list of count metrics to be emitted by a Wrangler entity (ex: {@link Directive}).
   * Note that this method doesn't emit metrics, it only returns metadata to be used in metrics emission logic elsewhere
   * @return list of {@link EntityCountMetric}s to be emitted
   */
  List<EntityCountMetric> getCountMetrics();
}
