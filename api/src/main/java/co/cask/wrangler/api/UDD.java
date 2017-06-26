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

import co.cask.wrangler.api.parser.UsageDefinition;

/**
 * UDD is a user defined directive. DIE - Define, Initialize & Execute - Pattern
 *
 * Following is a simple example of how to use this interface.
 * <code>
 *   @Plugin(type = UDD.Type)
 *   @Name("text-reverse")
 *   @Description("Reverses a string value of a column.")
 *   public final class TextReverse implements UDD {
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
 *     public List<Row> execute(List<Row> rows, RecipeContext context)
 *       throws DirectiveExecutionException, ErrorRecordException {
 *       ...
 *     }
 *   }
 * </code>
 */
public interface UDD extends Directive<Row, Row> {
  /**
   * This defines a interface variable that is static and final for specify
   * the {@code type} of the plugin this interface would provide.
   *
   * <p>This constant should be used when defining a plugin. An example
   * of such usage is as follow:</p>
   *
   * <code>
   *   @Plugin(type = UDD.Type)
   *   @Name("text-reverse")
   *   @Description("Reverses the value of the column.)
   *   public final class TextReverse implements UDD {
   *     ...
   *   }
   * </code>
   */
  String Type = "udd";

  /**
   * This method provides a way for the developer to provide information
   * about the arguments expected by this directive. The definition of
   * arguments would provide information to the framework about how each
   * argument should be parsed and interpretted.
   *
   * This method uses {@code UsageDefinition#Builder} to build the token
   * definitions.
   *
   * <code>
   *   UsageDefinition define() {
   *     UsageDefinition.Builder builder = UsageDefinition.builder();
   *     builder.define("column", TokeType.COLUMN_NAME); // :column
   *     builder.define("number", TokenType.NUMERIC, Optional.TRUE); // 1.0 or 8
   *     builder.define("text", TokenType.TEXT); // 'text'
   *     builder.define("boolean", TokenType.BOOL); // true / false
   *     builder.define("expression", TokenType.EXPRESSOION); // exp: { age < 10.0 }
   *   }
   * </code>
   *
   * @return A object of {@code UsageDefinition} containing definitions of each argument
   * expected by this directive.
   *
   * @see co.cask.wrangler.api.parser.TokenType
   */
  UsageDefinition define();

  /**
   *
   * @param args
   * @throws DirectiveParseException
   */
  void initialize(Arguments args) throws DirectiveParseException;
}