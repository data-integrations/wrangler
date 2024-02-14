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
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.ReportErrorAndProceed;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.Expression;
import io.cdap.wrangler.api.parser.Identifier;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.expression.EL;
import io.cdap.wrangler.expression.ELContext;
import io.cdap.wrangler.expression.ELException;
import io.cdap.wrangler.expression.ELResult;

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
public class SendToErrorAndContinue implements Directive, Lineage {
  public static final String NAME = "send-to-error-and-continue";
  private EL el;
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
      el = EL.compile(condition);
    } catch (ELException e) {
      throw new DirectiveParseException(
        NAME, String.format("Invalid condition '%s'.", condition), e);
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
      ELContext ctx = new ELContext(context, el, row);

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
        } else if (context != null && !context.getTransientStore().getVariables().contains("dq_failure")) {
            context.getTransientStore().set(TransientVariableScope.LOCAL, "dq_failure", 0L);
        }
      } catch (ELException e) {
        throw new DirectiveExecutionException(NAME, e.getMessage(), e);
      }
      results.add(row);
    }
    return results;
  }

  @Override
  public Mutation lineage() {
    Mutation.Builder builder = Mutation.builder()
      .readable("Redirect records to error path based on expression'%s'", condition);
    el.variables().forEach(column -> builder.relation(column, column));
    return builder.build();
  }
}
