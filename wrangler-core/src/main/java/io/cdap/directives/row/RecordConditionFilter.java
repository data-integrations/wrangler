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
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.Bool;
import io.cdap.wrangler.api.parser.Expression;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.expression.EL;
import io.cdap.wrangler.expression.ELContext;
import io.cdap.wrangler.expression.ELException;

import java.util.ArrayList;
import java.util.List;

/**
 * A Wrangle step for filtering rows based on the condition.
 *
 * <p>
 *   This step will evaluate the condition, if the condition evaluates to
 *   true, then the row will be skipped. If the condition evaluates to
 *   false, then the row will be accepted.
 * </p>
 */
@Plugin(type = Directive.TYPE)
@Name(RecordConditionFilter.NAME)
@Categories(categories = { "row", "data-quality"})
@Description("Filters rows based on condition type specified.")
public class RecordConditionFilter implements Directive {
  public static final String NAME = "filter-row";
  private String condition;
  private final EL el = new EL(new EL.DefaultFunctions());
  private boolean isTrue;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("condition", TokenType.EXPRESSION);
    builder.define("type", TokenType.BOOLEAN, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    isTrue = true;
    if (args.contains("type")) {
      isTrue = ((Bool) args.value("type")).value();
    }
    condition = ((Expression) args.value("condition")).value();
    try {
      el.compile(condition);
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
    List<Row> results = new ArrayList<>();
    for (Row row : rows) {
      // Move the fields from the row into the context.
      ELContext ctx = new ELContext(context);
      for (String var : el.variables()) {
        Object value = row.getValue(var);
        String strValue;
        // support numeric values by converting them to string
        if (value instanceof Number) {
          Number number = (Number) value;
          strValue = number.toString();
          ctx.set(var, strValue);
        } else {
          ctx.set(var, row.getValue(var));
        }
      }
      if (context != null) {
        for (String variable : context.getTransientStore().getVariables()) {
          ctx.set(variable, context.getTransientStore().get(variable));
        }
      }
      try {
        Boolean result = el.execute(ctx).getBoolean();
        if (!isTrue) {
          result = !result;
        }
        if (result) {
          continue;
        }
      } catch (ELException e) {
        throw new DirectiveExecutionException(NAME, e.getMessage(), e);
      }
      results.add(row);
    }
    return results;
  }
}
