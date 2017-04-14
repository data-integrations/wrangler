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

import java.util.Set;

/**
 * A step which has bounded output but may have unbounded input.
 */
public interface UnboundedInputStep<I, O, C> extends Step<I, O, C> {

  /**
   * Returns the {@code Set} of columns which are used as output for this step
   *
   * @return The {@code Set} of columns which are used as output for this step
   */
  Set<C> getBoundedOutputColumns();

  /**
   * Returns the output columns associated with a particular input column or {@code null} if an inputColumn does not
   * have any associated output columns
   *
   * @param inputColumn The input column to find the output columns for
   * @return The {@code Set} of output columns associated with an input column; {@code null} if there are no output
   *         columns associated
   */
  Set<C> getOutputColumn(C inputColumn);
}
