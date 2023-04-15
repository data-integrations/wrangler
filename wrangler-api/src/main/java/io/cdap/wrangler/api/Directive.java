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
 * Directive is a user defined directive. DIE - Define, Initialize & Execute - Pattern
 *
 * Following is a simple example of how to use this interface.
 * <code>
 *   @Plugin(type = Directive.Type)
 *   @Name("text-reverse")
 *   @Description("Reverses a string value of a column.")
 *   public final class TextReverse implements Directive {
 *     private final ColumnName columnArgs;
 *
 *     @Override
 *     public UsageDefinition define() {
 *       UsageDefinition.Builder builder = UsageDefinition.builder();
 *       builder.define("col", TokenType.COLUMN_NAME)
 *       return builder.build();
 *     }
 *
 *     @Override
 *     public void initialize(Argument args) throws DirectiveParseException {
 *      this.columnArgs = args.value("col");
 *     }
 *
 *     @Override
 *     public List<Row> execute(List<Row> rows, ExecutorContext context)
 *       throws DirectiveExecutionException, ErrorRowException {
 *       ...
 *     }
 *   }
 * </code>
 */
public interface Directive extends Executor<List<Row>, List<Row>>, EntityMetrics {
  /**
   * This defines a interface variable that is static and final for specify
   * the {@code type} of the plugin this interface would provide.
   *
   * <p>This constant should be used when defining a plugin. An example
   * of such usage is as follow:</p>
   *
   * <code>
   *   @Plugin(type = Directive.Type)
   *   @Name("text-reverse")
   *   @Description("Reverses the value of the column.)
   *   public final class TextReverse implements Directive {
   *     ...
   *   }
   * </code>
   */
  String TYPE = "directive";

  /**
   * This method provides a way for the developer to provide information
   * about the arguments expected by this directive. The definition of
   * arguments would provide information to the framework about how each
   * argument should be parsed and interpretted.
   *
   * <p>This method uses {@code UsageDefinition#Builder} to build the token
   * definitions. Each token definition consists of {@code name}, {@code TokenType}
   * and {@code optional} field that specifies whether the token specified is
   * optionally present.</p>
   *
   * <p>The {@code UsageDefinition} provides different methods to {@code define},
   * and as well generate {@code usage} based on the definition.</p>
   *
   * <p>This method is invoked by the framework at the time of creating an executable
   * directive that will be added to the {@link RecipePipeline}. It's generally during
   * the configuration phase.</p>.
   *
   * <p>NOTE: As best practice, developer needs to make sure that this class doesn't
   * throw an exception. Also, it should not include external libraries that can
   * generate exception unknown to the developer.</p>
   *
   * <p>
   *   Following is an example of how {@code define} could be used.
   * <code>
   *   public UsageDefinition define() {
   *     UsageDefinition.Builder builder = UsageDefinition.builder();
   *     builder.define("column", TokeType.COLUMN_NAME); // :column
   *     builder.define("number", TokenType.NUMERIC, Optional.TRUE); // 1.0 or 8
   *     builder.define("text", TokenType.TEXT); // 'text'
   *     builder.define("boolean", TokenType.BOOL); // true / false
   *     builder.define("expression", TokenType.EXPRESSOION); // exp: { age < 10.0 }
   *   }
   * </code>
   * </p>
   *
   * {@code TokenType} supports many different token types that can be used within the
   * usage definition.
   *
   * @return A object of {@code UsageDefinition} containing definitions of each argument
   * expected by this directive.
   *
   * @see io.cdap.wrangler.api.parser.TokenType
   */
  UsageDefinition define();

  /**
   * This method provides a way to emit metrics from the Directive. Metadata about each metric to be emitted can be
   * returned and used in the metrics emission logic elsewhere.
   * @return List of metrics ({@link EntityCountMetric}s) emitted by this directive
   */
  @Override
  default List<EntityCountMetric> getCountMetrics() {
    // no op
    return null;
  }
}
