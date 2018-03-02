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

package co.cask.directives.row;

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
import co.cask.wrangler.api.parser.Expression;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import co.cask.wrangler.expression.EL;
import co.cask.wrangler.expression.ELContext;
import co.cask.wrangler.expression.ELException;
import co.cask.wrangler.expression.ELResult;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A directive for erroring the processing if condition is set to true.
 */
@Plugin(type = Directive.Type)
@Name(Fail.NAME)
@Categories(categories = { "row", "data-quality"})
@Description("Fails when the condition is evaluated to true.")
public class Fail implements Directive {
  public static final String NAME = "fail";
  private String condition;
  private final EL el = new EL(new EL.DefaultFunctions());

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("condition", TokenType.EXPRESSION);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    Expression expression = args.value("condition");
    if(expression.value().isEmpty()) {
      throw new DirectiveParseException(
        String.format("No condition has been specified.")
      );
    }
    condition = expression.value();
    try {
      el.compile(condition);
    } catch (ELException e) {
      throw new DirectiveParseException(e.getMessage());
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException {
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
        if (result.getBoolean()) {
          throw new DirectiveExecutionException(
            String.format("Condition '%s' evaluated to true. Terminating processing.", condition)
          );
        }
      } catch (ELException e) {
        throw new DirectiveExecutionException(e.getMessage());
      }
    }
    return rows;
  }
}
