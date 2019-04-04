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

package io.cdap.directives.row;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.Expression;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.expression.EL;
import io.cdap.wrangler.expression.ELContext;
import io.cdap.wrangler.expression.ELException;
import io.cdap.wrangler.expression.ELResult;

import java.util.List;

/**
 * A directive for erroring the processing if condition is set to true.
 */
@Plugin(type = Directive.TYPE)
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
    if (expression.value().isEmpty()) {
      throw new DirectiveParseException("No condition has been specified.");
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
      for (String var : el.variables()) {
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
