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
import co.cask.wrangler.api.pipeline.PipelineContext;

import java.io.Serializable;
import java.util.List;

/**
 * A interface defining the wrangle Directive in the wrangling {@link RecipePipeline}.
 */
@PublicEvolving
public interface Directive<I, O> extends Serializable {
  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records List of input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled List of {@link Record}.
   */
  List<O> execute(List<I> records, PipelineContext context)
    throws DirectiveExecutionException, ErrorRecordException;
}

