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
 *
 * @param <I> type of input object
 * @param <O> type of output object
 * @param <E> type of error object
 */
@PublicEvolving
public interface RecipePipeline<I, O, E> extends Serializable, AutoCloseable {

  /**
   * Executes the pipeline on the input.
   *
   * @param input List of Input record of type I.
   * @param schema Schema to which the output should be mapped.
   * @return Parsed output list of record of type O
   */
  List<O> execute(List<I> input, Schema schema) throws RecipeException;

  /**
   * Executes the pipeline on the input.
   *
   * @param input List of input record of type I.
   * @return Parsed output list of record of type I
   */
  List<I> execute(List<I> input) throws RecipeException;

  /**
   * Returns records that are errored out.
   *
   * @return records that have errored out.
   */
  List<E> errors();

  /**
   * Destroys the pipeline.
   */
  @Override
  void close();
}


