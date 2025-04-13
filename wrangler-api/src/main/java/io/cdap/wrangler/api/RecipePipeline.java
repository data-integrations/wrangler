/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.io.Serializable;
import java.util.List;

/**
 * {@link RecipePipeline} executes array of {@link Executor} in the order they are specified.
 * The pipeline provides error handling and schema mapping capabilities.
 *
 * @param <I> type of input object
 * @param <O> type of output object
 * @param <E> type of error object
 */
@PublicEvolving
public interface RecipePipeline<I, O, E> extends Serializable, AutoCloseable {

  /**
   * Executes the pipeline on the input with schema mapping.
   *
   * @param input List of Input record of type I
   * @param schema Schema to which the output should be mapped
   * @return Parsed output list of record of type O
   * @throws RecipeException if there is an error during pipeline execution
   *         or schema mapping
   */
  List<O> execute(List<I> input, Schema schema) throws RecipeException;

  /**
   * Executes the pipeline on the input without schema mapping.
   *
   * @param input List of input record of type I
   * @return Parsed output list of record of type I
   * @throws RecipeException if there is an error during pipeline execution
   */
  List<I> execute(List<I> input) throws RecipeException;

  /**
   * Returns records that encountered errors during processing.
   * These records were not successfully transformed by the pipeline.
   *
   * @return List of error records of type E
   */
  List<E> errors();

  /**
   * Releases any resources held by this pipeline.
   * This method should be called when the pipeline is no longer needed.
   */
  @Override
  void close();
}


