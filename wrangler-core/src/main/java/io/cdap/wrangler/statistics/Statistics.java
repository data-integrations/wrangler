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

package io.cdap.wrangler.statistics;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.util.List;

/**
 * Interface for calculating metrics over the records being processed.
 */
@PublicEvolving
public interface Statistics {
  /**
   * Aggregates statistics for all the rows.
   *
   * @param rows to be aggregated.
   * @return Summary in the form of {@link Row}
   */
  Row aggregate(List<Row> rows);
}
