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
 * A step which has bounded input but may have unbounded output.
 */
public interface UnboundedOutputStep<I, O, C> extends Step<I, O, C> {

  /**
   * Returns the {@code Set} of columns which are used as input for this step
   *
   * @return The {@code Set} of columns which are used as input for this step
   */
  Set<C> getBoundedInputColumns();

  /**
   * Returns {@code true} if {@code column} is possible output from this step
   *
   * @param column The column to test if this is possible output
   * @return {@code true} if {@code column} is possible output from this step, otherwise {@code false}
   */
  boolean isOutput(C column);
}
