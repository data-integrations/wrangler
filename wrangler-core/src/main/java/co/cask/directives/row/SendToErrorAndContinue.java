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
import co.cask.wrangler.api.Optional;
import co.cask.wrangler.api.ReportErrorAndProceed;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.TransientVariableScope;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.Expression;
import co.cask.wrangler.api.parser.Identifier;
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import co.cask.wrangler.expression.EL;
import co.cask.wrangler.expression.ELContext;
import co.cask.wrangler.expression.ELException;
import co.cask.wrangler.expression.ELResult;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive for erroring the record if
 *
 * <p>
 *   This step will evaluate the condition, if the condition evaluates to
 *   true, then the row will be skipped. If the condition evaluates to
 *   false, then the row will be accepted.
 * </p>
 */
@Plugin(type = Directive.TYPE)
@Name(SendToErrorAndContinue.NAME)
@Categories(categories = { "row", "data-quality"})
@Description("Send records that match condition to the error collector and continues processing.")
public class SendToErrorAndContinue implements Directive {
  public static final String NAME = "send-to-error-and-continue";
  private final EL el = new EL(new EL.DefaultFunctions());
  private String condition;
  private String metric = null;
  private String message = null;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("condition", TokenType.EXPRESSION);
    builder.define("metric", TokenType.IDENTIFIER, Optional.TRUE);
    builder.define("message", TokenType.TEXT, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    condition = ((Expression) args.value("condition")).value();
    try {
      el.compile(condition);
    } catch (ELException e) {
      throw new DirectiveParseException(
        String.format("Invalid condition '%s'.", condition)
      );
    }
    if (args.contains("metric")) {
      metric = ((Identifier) args.value("metric")).value();
    }
    if (args.contains("message")) {
      message = ((Text) args.value("message")).value();
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException, ReportErrorAndProceed {
    if (context != null) {
      context.getTransientStore().increment(TransientVariableScope.LOCAL, "dq_total", 1);
    }
    List<Row> results = new ArrayList<>();
    for (Row row : rows) {
      // Move the fields from the row into the context.
      ELContext ctx = new ELContext(context);
      ctx.set("this", row);
      for (String var : el.variables()) {
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
          if (metric != null && context != null) {
            context.getMetrics().count(metric, 1);
          }
          if (message == null) {
            message = condition;
          }
          if (context != null) {
            context.getTransientStore().increment(TransientVariableScope.LOCAL, "dq_failure", 1);
          }
          throw new ReportErrorAndProceed(message, 1);
        }
      } catch (ELException e) {
        throw new DirectiveExecutionException(e.getMessage());
      }
      results.add(row);
    }
    return results;
  }
}
