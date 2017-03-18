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
 * A step in a wrangling pipeline which may have unbounded input and output columns.
 */
public interface UnboundedInputOutputStep<I, O, C> extends Step<I, O, C> {

  /**
   * Returns the set of input columns which are used to produce an output column given that output column or
   * {@code null} if this {@code Step} does not produce it as an output column.
   *
   * @param outputColumn The output column to find the input columns of
   * @return The {@code Set} of input columns that are used to produce the {@code outputColumn}; null if this step does
   *         not produce this {@code outputColumn}
   */
  Set<C> getInputColumns(C outputColumn);
}
