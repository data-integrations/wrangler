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

import co.cask.cdap.api.data.schema.Schema;

/**
 * Wrangle Pipeline executes steps in the order they are specified.
 */
public interface Pipeline<I,O> {
  /**
   * Configures the wrangle pipeline using the specification.
   *
   * @param specification Wrangle specification.
   */
  public void configure(Specification specification, PipelineContext context);

  /**
   * Executes the pipeline on the input.
   *
   * @param input Input record of type I
   * @param schema Schema to which the output should be mapped.
   * @return Parsed output record of type O
   */
  public O execute(I input, Schema schema) throws PipelineException, SkipRowException;
}

