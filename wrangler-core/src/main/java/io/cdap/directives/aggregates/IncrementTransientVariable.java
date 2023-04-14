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

package io.cdap.directives.aggregates;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.EntityCountMetric;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.Expression;
import io.cdap.wrangler.api.parser.Identifier;
import io.cdap.wrangler.api.parser.Numeric;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.expression.EL;
import io.cdap.wrangler.expression.ELContext;
import io.cdap.wrangler.expression.ELException;
import io.cdap.wrangler.expression.ELResult;

import java.util.List;

import static io.cdap.wrangler.metrics.JexlCategoryMetricUtils.getJexlCategoryMetric;

/**
 * A directive for incrementing the a transient variable based on conditions.
 */
@Plugin(type = Directive.TYPE)
@Name(IncrementTransientVariable.NAME)
@Categories(categories = { "transient"})
@Description("Wrangler - A interactive tool for data cleansing and transformation.")
public class IncrementTransientVariable implements Directive {
  public static final String NAME = "increment-variable";
  private String variable;
  private long incrementBy;
  private EL el;

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
    this.incrementBy = ((Numeric) args.value("value")).value().longValue();
    String expression = ((Expression) args.value("condition")).value();
    try {
      el = EL.compile(expression);
    } catch (ELException e) {
      throw new DirectiveParseException(NAME, e.getMessage(), e);
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      // Move the fields from the row into the context.
      ELContext ctx = new ELContext();
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
          context.getTransientStore().increment(TransientVariableScope.GLOBAL, variable, incrementBy);
        }
      } catch (ELException e) {
        throw new DirectiveExecutionException(NAME, e.getMessage(), e);
      }
    }
    return rows;
  }

  public void destroy() {
    // no-op
  }

  public List<EntityCountMetric> getCountMetrics() {
    EntityCountMetric jexlCategoryMetric = getJexlCategoryMetric(el.getScriptParsedText());
    return (jexlCategoryMetric == null) ? null : ImmutableList.of(jexlCategoryMetric);
  }
}
