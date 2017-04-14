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

import java.util.List;

/**
 * Optimizes a {@code List} of {@code Step}s into a more efficient form.
 */
public interface Optimizer<I, O, C> {

  /**
   * Optimizes a {@code List} of {@code Step}s and returns it.
   *
   * @param steps The {@code List} of {@code Step}s to be optimized.
   * @return The optimized {@code List} of steps.
   */
  List<Step<I, O, C>> optimize(List<Step<I, O, C>> steps);
}
