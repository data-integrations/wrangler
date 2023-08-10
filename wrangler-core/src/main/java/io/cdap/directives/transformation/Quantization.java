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

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.InvalidRelation;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.RelationalTranformContext;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.Triplet;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Numeric;
import io.cdap.wrangler.api.parser.Ranges;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.utils.SqlExpressionGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A Wrangler step for quantizing a column.
 */
@Plugin(type = Directive.TYPE)
@Name(Quantization.NAME)
@Categories(categories = { "transform"})
@Description("Quanitize the range of numbers into label values.")
public class Quantization implements Directive, Lineage {
  public static final String NAME = "quantize";
  private final RangeMap<Double, String> rangeMap = TreeRangeMap.create();
  private String col1;
  private String col2;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("source", TokenType.COLUMN_NAME);
    builder.define("destination", TokenType.COLUMN_NAME);
    builder.define("ranges", TokenType.RANGES);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.col1 = ((ColumnName) args.value("source")).value();
    this.col2 = ((ColumnName) args.value("destination")).value();
    List<Triplet<Numeric, Numeric, String>> ranges = ((Ranges) args.value("ranges")).value();
    for (Triplet<Numeric, Numeric, String> range : ranges) {
      double lower = range.getFirst().value().doubleValue();
      double upper = range.getSecond().value().doubleValue();
      rangeMap.put(Range.closed(lower, upper), range.getThird());
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
      int idx = row.find(col1);

      if (idx != -1) {
        try {
          Object object = row.getValue(idx);

          if (object == null) {
            throw new DirectiveExecutionException(
              NAME, String.format("Column '%s' has null value. It should be a non-null 'String', " +
                                    "'Float' or 'Double'.", col1));
          }

          Double d;
          if (object instanceof String) {
            d = Double.parseDouble((String) object);
          } else if (object instanceof Double) {
            d = (Double) object;
          } else if (object instanceof Float) {
            d = ((Float) object).doubleValue();
          } else {
            throw new DirectiveExecutionException(
              NAME, String.format("Column '%s' has invalid type '%s'. It should be of type 'String', " +
                                    "'Float' or 'Double'.", col1, object.getClass().getSimpleName()));
          }
          String value = rangeMap.get(d);
          int destIdx = row.find(col2);
          if (destIdx == -1) {
            row.add(col2, value);
          } else {
            row.setValue(destIdx, value);
          }
        } catch (NumberFormatException e) {
          throw new DirectiveExecutionException(
            NAME, String.format("Column '%s' has invalid type. It should be of type 'String', " +
                                  "'Float' or 'Double'.", col1), e);
        }
      } else {
        throw new DirectiveExecutionException(NAME, "Column '" + col1 + "' does not exist.");
      }
      results.add(row);
    }
    return results;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Quanitized column '%s' into column '%s'", col1, col2)
      .conditional(col1, col2)
      .build();
  }

  @Override
  public Relation transform(RelationalTranformContext relationalTranformContext, Relation relation) {
    Optional<ExpressionFactory<String>> expressionFactory =
            SqlExpressionGenerator.getExpressionFactory(relationalTranformContext);
    if (!expressionFactory.isPresent()) {
      return new InvalidRelation("Cannot find an Expression Factory");
    }
    List<String> caseStatements = new ArrayList<>();
    this.rangeMap.asMapOfRanges().entrySet().stream()
            .forEach(entry -> caseStatements.add(String.format("WHEN double(%s) BETWEEN %f AND %f THEN '%s'", col1,
                    entry.getKey().lowerEndpoint(), entry.getKey().upperEndpoint(), entry.getValue())));

    return relation.setColumn(col2, expressionFactory.get().compile(String.format(
            "CASE %s ELSE NULL END", String.join(" ", caseStatements))));
  }

  @Override
  public boolean isSQLSupported() {
    return true;
  }
}
