/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler.api;

import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.List;

/**
 * Directive is a user defined directive that follows the DIE (Define, Initialize &amp; Execute) pattern.
 *
 * <p>Following is a simple example of how to use this interface:</p>
 * <pre>{@code
 *   @Plugin(type = Directive.TYPE_ANNOTATION)
 *   @Name("text-reverse")
 *   @Description("Reverses a string value of a column.")
 *   public final class TextReverse implements Directive {
 *     private final ColumnName columnArgs;
 *
 *     @Override
 *     public UsageDefinition define() {
 *       UsageDefinition.Builder builder = UsageDefinition.builder();
 *       builder.define("col", TokenType.COLUMN_NAME);
 *       return builder.build();
 *     }
 *
 *     @Override
 *     public void initialize(Arguments args) throws DirectiveParseException {
 *       this.columnArgs = args.value("col");
 *     }
 *
 *     @Override
 *     public List<Row> execute(List<Row> rows, ExecutorContext context)
 *       throws DirectiveExecutionException, ErrorRowException {
 *       // Implementation here
 *     }
 *   }
 * }</pre>
 */
public interface Directive extends Executor<List<Row>, List<Row>>, EntityMetrics {
  /**
   * The plugin type identifier for directives. This constant should be used when defining a plugin.
   */  String TYPE = "directive";
  
  /**
   * Convenience constant for use in annotations.
   */
  String TYPE_ANNOTATION = TYPE;

  /**
   * Defines the arguments expected by this directive.
   * 
   * <p>This method uses {@link UsageDefinition.Builder} to build token definitions. Each token 
   * definition consists of a name, {@link TokenType}, and optional flag that specifies whether 
   * the token is required.</p>
   *
   * <p>The {@link UsageDefinition} provides methods to define tokens and generate usage 
   * documentation based on the definition.</p>
   *
   * <p>This method is called during directive initialization when creating an executable directive 
   * for the {@link RecipePipeline}.</p>
   *
   * <p>Best practices:</p>
   * <ul>
   *   <li>This method should not throw exceptions</li>
   *   <li>Avoid using external libraries that may throw unexpected exceptions</li>
   * </ul>
   *
   * <p>Example usage:</p>
   * <pre>{@code
   *   public UsageDefinition define() {
   *     UsageDefinition.Builder builder = UsageDefinition.builder();
   *     builder.define("column", TokenType.COLUMN_NAME);     // :column
   *     builder.define("number", TokenType.NUMERIC, true);   // Optional: 1.0 or 8
   *     builder.define("string", TokenType.TEXT);            // 'text'
   *     return builder.build();
   *   }
   * }</pre>
   *
   * @return A {@link UsageDefinition} object defining the directive's arguments
   */
  UsageDefinition define();

  /**
   * Initializes the directive with parsed arguments.
   *
   * <p>This method is called after argument parsing and before execution. It should
   * validate and store the parsed arguments for use during execution.</p>
   *
   * @param args The parsed arguments for this directive
   * @throws DirectiveParseException if the arguments are invalid or cannot be processed
   */
  void initialize(Arguments args) throws DirectiveParseException;

  /**
   * Executes the directive on a batch of rows.
   *
   * <p>This method implements the actual data transformation logic.</p>
   *
   * @param rows List of input rows to process
   * @param context Execution context providing runtime information and services
   * @return List of transformed rows
   * @throws DirectiveExecutionException if there is an error during execution
   * @throws ErrorRowException if specific rows cannot be processed
   */
  @Override
  List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException, ErrorRowException;
}
