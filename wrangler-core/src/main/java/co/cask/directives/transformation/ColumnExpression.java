/*
 *  Copyright © 2017 Cask Data, Inc.
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

package co.cask.directives.transformation;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Expression;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import co.cask.wrangler.expression.EL;
import co.cask.wrangler.expression.ELContext;
import co.cask.wrangler.expression.ELException;
import co.cask.wrangler.expression.ELResult;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A directive for apply an expression to store the result in a column.
 *
 * The expressions are specified in JEXL format (http://commons.apache.org/proper/commons-jexl/reference/syntax.html)
 * Executor is response for executing only one expression for each {@link Row} record that is
 * being passed. The result of expression either adds a new column or replaces the value of
 * the existing column.
 *
 * <p>
 *   Executor step = new ColumnExpression(lineno, directive, column, "if (age > 24 ) { 'adult' } else { 'teen' }");
 * </p>
 */
@Plugin(type = Directive.Type)
@Name(ColumnExpression.NAME)
@Categories(categories = { "transform"})
@Description("Sets a column by evaluating a JEXL expression.")
public class ColumnExpression implements Directive {
  public static final String NAME = "set-column";
  // Column to which the result of experience is applied to.
  private String column;
  // The actual expression
  private String expression;
  // Properties associated with pipeline
  private final Map<String, Object> properties = new HashMap<>();
  private final EL el = new EL(new EL.DefaultFunctions());

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("expression", TokenType.EXPRESSION);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.expression = ((Expression) args.value("expression")).value();
    try {
      el.compile(expression);
    } catch (ELException e ){
      throw new DirectiveParseException(e.getMessage());
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      // Move the fields from the row into the context.
      ELContext ctx = new ELContext(context);
      ctx.set("this", row);
      for(String var : el.variables()) {
        ctx.set(var, row.getValue(var));
      }

      // Execution of the script / expression based on the row data
      // mapped into context.
      try {
        ELResult result = el.execute(ctx);
        int idx = row.find(this.column);
        if (idx == -1) {
          row.add(this.column, result.getObject());
        } else {
          row.setValue(idx, result.getObject());
        }
      } catch (ELException e) {
        throw new DirectiveExecutionException(e.getMessage());
      }
    }
    return rows;
  }
}


