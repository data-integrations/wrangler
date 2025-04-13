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
 * Interface defining the execution step in a {@link RecipePipeline}.
 * Executors process data by applying transformations specified in directives.
 *
 * @param <I> Type of input data to be processed
 * @param <O> Type of output data after processing
 */
@PublicEvolving
public interface Executor<I, O> extends Serializable {
  /**
   * Initializes the executor with parsed arguments from the directive.
   * This method is called once during initialization before any data processing begins.
   *
   * <p>Arguments are constructed based on the definition provided in the {@code define} method.
   * The method should validate arguments and store them for use during execution.</p>
   *
   * <p>Example usage:</p>
   * <pre>{@code
   * public void initialize(Arguments args) throws DirectiveParseException {
   *   ColumnName column = args.value("column");
   *   if (args.contains("number")) {
   *     Numeric number = args.value("number");
   *   }
   *   Text text = args.value("text");
   *   Bool bool = args.value("boolean");
   *   Expression expression = args.value("expression");
   * }
   * }</pre>
   *
   * @param args Parsed and validated arguments for this directive
   * @throws DirectiveParseException if argument validation fails or values are invalid
   */
  void initialize(Arguments args) throws DirectiveParseException;

  /**
   * Executes the directive's transformation on input data.
   *
   * <p>This method implements the actual data processing logic defined by the directive.
   * It may transform, filter, or aggregate the input data to produce the output.</p>
   *
   * @param rows Input data to be transformed
   * @param context Execution context providing runtime information and services
   * @return Transformed output data
   * @throws DirectiveExecutionException if there is an error during execution
   * @throws ErrorRowException if specific rows cannot be processed
   * @throws ReportErrorAndProceed if there are non-fatal errors that should be reported
   */
  O execute(I rows, ExecutorContext context)
    throws DirectiveExecutionException, ErrorRowException, ReportErrorAndProceed;

  /**
   * Performs cleanup when this executor is being shut down.
   * 
   * <p>This method is called during:</p>
   * <ul>
   *   <li>{@code Transform#destroy()} when the directive is used in a plugin</li>
   *   <li>{@code Service#destroy()} when used in a service</li>
   * </ul>
   *
   * <p>Implementations should release any resources acquired during initialization.
   * This method must not throw exceptions as they cannot be handled properly during
   * shutdown.</p>
   */
  void destroy();

  /**
   * Gets the output schema after this directive's transformation is applied.
   *
   * <p>This method helps pipeline planning by describing how the directive modifies
   * the structure of the data. Override this method if your directive:</p>
   * <ul>
   *   <li>Renames columns</li>
   *   <li>Changes column data types</li>
   *   <li>Adds new columns with specific schemas</li>
   * </ul>
   *
   * @param schemaResolutionContext Context containing schema resolution information
   * @return Schema of the transformed data, or null if schema should be inferred
   */
  @Nullable
  default Schema getOutputSchema(SchemaResolutionContext schemaResolutionContext) {
    return null;
  }
}
