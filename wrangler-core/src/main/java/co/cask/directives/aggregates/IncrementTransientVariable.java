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

package co.cask.directives.aggregates;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ErrorRowException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.Expression;
import co.cask.wrangler.api.parser.Identifier;
import co.cask.wrangler.api.parser.Numeric;
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
 * A directive for incrementing the a transient variable based on conditions.
 */
@Plugin(type = Directive.Type)
@Name(IncrementTransientVariable.NAME)
@Categories(categories = { "transient"})
@Description("Wrangler - A interactive tool for data cleansing and transformation.")
public class IncrementTransientVariable implements Directive {
  public static final String NAME = "increment-variable";
  private String variable;
  private long incrementBy;
  private String expression;
  private final EL el = new EL(new EL.DefaultFunctions());

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("variable", TokenType.IDENTIFIER);
    builder.define("value", TokenType.NUMERIC);
    builder.define("condition", TokenType.EXPRESSION);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.variable = ((Identifier) args.value("variable")).value();
    this.expression = ((Expression) args.value("condition")).value();
    this.incrementBy = ((Numeric) args.value("value")).value().longValue();
    try {
      el.compile(expression);
    } catch (ELException e) {
      throw new DirectiveParseException(e.getMessage());
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException, ErrorRowException {
    for (Row row : rows) {
      // Move the fields from the row into the context.
      ELContext ctx = new ELContext();
      ctx.set("this", row);
      for(String var : el.variables()) {
        ctx.set(var, row.getValue(var));
      }

      // Transient variables are added.
      if (context != null) {
        for (String variable : context.getTransientStore().getVariables()) {
          ctx.set(variable, context.getTransientStore().get(variable));
        }
      }

      // Execution of the script / expression based on the row data
      // mapped into context.
      try {
        ELResult result = el.execute(ctx);
        if (result.getBoolean()) {
          context.getTransientStore().increment(variable, incrementBy);
        }
      } catch (ELException e) {
        throw new DirectiveExecutionException(e.getMessage());
      }
    }
    return rows;
  }

  public void destroy() {
    // no-op
  }
}
