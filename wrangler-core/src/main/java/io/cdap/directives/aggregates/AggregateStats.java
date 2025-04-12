/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  either express or implied. See the License for the specific
 *  language governing permissions and limitations under the License.
 */

package io.cdap.directives.aggregates;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * Directive to calculate min, max, sum, avg, count for a given column.
 */
public class AggregateStats implements Directive {

  private String columnName;

  @Override
  public void initialize(Arguments arguments) throws DirectiveParseException {
    if (arguments.size() != 1) {
      throw new DirectiveParseException("AggregateStats directive requires exactly one argument: column name");
    }
    this.columnName = ((Text) arguments.value("column")).value();
  }

  @Override
  public UsageDefinition define() {
    return UsageDefinition.builder("aggregate-stats").build();
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    Double min = null, max = null, sum = 0.0;
    long count = 0;

    for (Row row : rows) {
      Object value = row.getValue(columnName);
      if (value == null) {
        continue;
      }

      double number;
      try {
        number = Double.parseDouble(value.toString());
      } catch (NumberFormatException e) {
        throw new DirectiveExecutionException("Invalid data in column: " + columnName);
      }

      min = (min == null) ? number : Math.min(min, number);
      max = (max == null) ? number : Math.max(max, number);
      sum += number;
      count++;
    }

    if (count == 0) {
      throw new DirectiveExecutionException("No valid data found in column: " + columnName);
    }

    double avg = sum / count;

    Row result = new Row();
    result.add("min", min);
    result.add("max", max);
    result.add("sum", sum);
    result.add("avg", avg);
    result.add("count", count);

    List<Row> output = new ArrayList<>();
    output.add(result);
    return output;
  }

  @Override
  public void destroy() {
    // Nothing to cleanup

  }

}
