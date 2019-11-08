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
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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
@Name(SendToError.NAME)
@Categories(categories = { "row", "data-quality"})
@Description("Send records that match condition to the error collector.")
public class SendToError implements Directive {
  public static final String NAME = "send-to-error";
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
  public List<FieldTransformOperation> getFieldOperation() {
    return el.variables().stream().map(
      variable -> new FieldTransformOperation(String.format("Send to error for column %s", variable),
                                              String.format("Send to error based on column %s", variable),
                                              Collections.singletonList(variable), variable))
             .collect(Collectors.toList());
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    condition = ((Expression) args.value("condition")).value();
    try {
      el.compile(condition);
    } catch (ELException e) {
      throw new DirectiveParseException(
        NAME, String.format(" Invalid condition '%s'.", condition)
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
    throws DirectiveExecutionException, ErrorRowException {
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
          throw new ErrorRowException(NAME, message, 1);
        }
      } catch (ELException e) {
        throw new DirectiveExecutionException(NAME, e.getMessage(), e);
      }
      results.add(row);
    }
    return results;
  }
}
