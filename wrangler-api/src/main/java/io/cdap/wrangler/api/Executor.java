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
import javax.annotation.Nullable;

/**
 * A interface defining the wrangle Executor in the wrangling {@link RecipePipeline}.
 *
 * @param <I> type of input object
 * @param <O> type of output object
 */
@PublicEvolving
public interface Executor<I, O> extends Serializable {
  /**
   * This method provides a way for the custom directive writer the ability to access
   * the arguments passed by the users.
   *
   * <p>This method is invoked only once during the initialization phase of the {@code Executor}
   * object. The arguments are constructed based on the definition as provided by the user in
   * the method above {@code define}.</p>
   *
   * <p>
   *   Following is an example of how {@code initialize} could be used to accept the
   *   arguments that are tokenized and parsed by the framework.
   *   <code>
   *     public void initialize(Arguments args) throws DirectiveParseException {
   *       ColumnName column = args.value("column");
   *       if (args.contains("number") {
   *        Numeric number = args.value("number");
   *       }
   *       Text text = args.value("text");
   *       Bool bool = args.value("boolean");
   *       Expression expression = args.value("expression");
   *     }
   *   </code>
   * </p>
   *
   * @param args Tokenized and parsed arguments.
   * @throws DirectiveParseException thrown by the user in case of any issues with validation or
   * ensuring the argument values are as expected.
   */
  void initialize(Arguments args) throws DirectiveParseException;

  /**
   * Executes a wrangle step on single {@link Row} and return an array of wrangled {@link Row}.
   *
   * @param rows List of input {@link Row} to be wrangled by this step.
   * @param context {@link ExecutorContext} passed to each step.
   * @return Wrangled List of {@link Row}.
   */
  O execute(I rows, ExecutorContext context)
    throws DirectiveExecutionException, ErrorRowException, ReportErrorAndProceed;

  /**
   * This method provides a way for the directive to de-initialize or destroy the
   * resources that were acquired during the initialization phase. This method is
   * called from the <code>Transform#destroy()</code> when the directive is invoked
   * within a plugin or when during <code>Service#destroy()</code> when invoked in the
   * service.
   *
   * This method is specifically designed not to thrown any exceptions. So, if the
   * the user code is throws any exception, the system will be unable to react or
   * correct at this phase of invocation.
   */
  void destroy();

  /**
   * This method is used to get the updated schema of the data after the directive's transformation has been applied.
   *
   * @param schemaResolutionContext context containing necessary information for getting output schema
   * @return output {@link Schema} of the transformed data
   * @implNote By default, returns a null and the schema is inferred from the data when necessary.
   * <p>For consistent handling, override for directives that perform column renames,
   * column data type changes or column additions with specific schemas.</p>
   */
  @Nullable
  default Schema getOutputSchema(SchemaResolutionContext schemaResolutionContext) {
    // no op
    return null;
  }
}
