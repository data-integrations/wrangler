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

package co.cask.wrangler.api;

import java.util.Map;
import java.util.Set;

/**
 * A simple step in a wrangling pipeline which has bounded input and output columns.
 */
public interface SimpleStep<I, O, C> extends Step<I, O, C> {

  /**
   * Returns a {@code Map} from an output column to the input columns that are used to produce it.
   *
   * @return A {@code Map} from an output column to the input columns that are used to produce it
   */
  Map<C, Set<C>> getColumnMap();
}
