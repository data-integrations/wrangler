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
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.expression.EL;
import io.cdap.wrangler.expression.ELContext;
import io.cdap.wrangler.expression.ELException;
import io.cdap.wrangler.expression.ELResult;

import java.util.List;

import static io.cdap.wrangler.metrics.JexlCategoryMetricUtils.getJexlCategoryMetric;

/**
 * A directive that defines a transient variable who's life-expectancy is only within the record.
 *
 * The value set as transient variable is available to all the directives after that. But, it's
 * not available beyond the input record.
 */
@Plugin(type = Directive.TYPE)
@Name(SetTransientVariable.NAME)
@Categories(categories = { "transient"})
@Description("Sets the value for a transient variable for the record being processed.")
public class SetTransientVariable implements Directive {
  public static final String NAME = "set-variable";
  private EL el;
  private String variable;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("variable", TokenType.IDENTIFIER);
    builder.define("condition", TokenType.EXPRESSION);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.variable = ((Identifier) args.value("variable")).value();
    String expression = ((Expression) args.value("condition")).value();
    try {
      el = EL.compile(expression);
    } catch (ELException e) {
      throw new DirectiveParseException(NAME, e.getMessage(), e);
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
      ELContext ctx = new ELContext(context, el, row);

      // Execution of the script / expression based on the row data
      // mapped into context.
      try {
        ELResult result = el.execute(ctx);
        if (context != null) {
          context.getTransientStore().set(TransientVariableScope.GLOBAL, variable, result.getObject());
        }
      } catch (ELException e) {
        throw new DirectiveExecutionException(NAME, e.getMessage(), e);
      }
    }
    return rows;
  }

  @Override
  public List<EntityCountMetric> getCountMetrics() {
    EntityCountMetric jexlCategoryMetric = getJexlCategoryMetric(el.getScriptParsedText());
    return (jexlCategoryMetric == null) ? null : ImmutableList.of(jexlCategoryMetric);
  }
}
