/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.wrangler.api.annotations.PublicEvolving;

import java.io.Serializable;

/**
 * A interface defining the wrangle Executor in the wrangling {@link RecipePipeline}.
 */
@PublicEvolving
public interface Executor<I, O> extends Serializable {
  /**
   * Executes a wrangle step on single {@link Row} and return an array of wrangled {@link Row}.
   *
   * @param rows List of input {@link Row} to be wrangled by this step.
   * @param context {@link ExecutorContext} passed to each step.
   * @return Wrangled List of {@link Row}.
   */
  O execute(I rows, ExecutorContext context)
    throws DirectiveExecutionException, ErrorRowException;
}

