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

package io.cdap.directives.transformation;

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
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Many;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Expression;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.expression.EL;
import io.cdap.wrangler.expression.ELContext;
import io.cdap.wrangler.expression.ELException;
import io.cdap.wrangler.expression.ELResult;

import java.util.List;

import static io.cdap.wrangler.metrics.JexlCategoryMetricUtils.getJexlCategoryMetric;

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
@Plugin(type = Directive.TYPE)
@Name(ColumnExpression.NAME)
@Categories(categories = { "transform"})
@Description("Sets a column by evaluating a JEXL expression.")
public class ColumnExpression implements Directive, Lineage {
  public static final String NAME = "set-column";
  // Column to which the result of experience is applied to.
  private String column;
  // The actual expression
  private String expression;
  private EL el;

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
        int idx = row.find(this.column);
        if (idx == -1) {
          row.add(this.column, result.getObject());
        } else {
          row.setValue(idx, result.getObject());
        }
      } catch (ELException e) {
        throw new DirectiveExecutionException(NAME, e.getMessage(), e);
      }
    }
    return rows;
  }

  @Override
  public Mutation lineage() {
    Mutation.Builder builder = Mutation.builder()
      .readable("Mapped result of expression '%s' to column '%s'", expression, column);
    builder.relation(Many.of(el.variables()), column);
    el.variables().forEach(col -> {
      if (!col.equals(column)) {
        builder.relation(col, col);
      }
    });
    return builder.build();
  }

  @Override
  public List<EntityCountMetric> getCountMetrics() {
    EntityCountMetric jexlCategoryMetric = getJexlCategoryMetric(el.getScriptParsedText());
    return (jexlCategoryMetric == null) ? null : ImmutableList.of(jexlCategoryMetric);
  }
}
