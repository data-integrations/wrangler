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

package co.cask.directives.transformation;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.Triplet;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Numeric;
import co.cask.wrangler.api.parser.Ranges;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import java.util.ArrayList;
import java.util.List;

/**
 * A Wrangler step for quantizing a column.
 */
@Plugin(type = Directive.Type)
@Name(Quantization.NAME)
@Categories(categories = { "transform"})
@Description("Quanitize the range of numbers into label values.")
public class Quantization implements Directive {
  public static final String NAME = "quantize";
  private static final String RANGE_PATTERN="([+-]?\\d+(?:\\.\\d+)?):([+-]?\\d+(?:\\.\\d+)?)=(.[^,]*)";
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
          Double d;
          if (object instanceof String) {
            d = Double.parseDouble((String) object);
          } else if (object instanceof Double) {
            d = (Double) object;
          } else if (object instanceof Float) {
            d = ((Float) object).doubleValue();
          } else {
            throw new DirectiveExecutionException(
              String.format("%s : Invalid type '%s' of column '%s'. Should be of type String, Float or Double.",
                            toString(), object != null ? object.getClass().getName() : "null", col1)
            );
          }
          String value = rangeMap.get(d);
          int destIdx = row.find(col2);
          if (destIdx == -1) {
            row.add(col2, value);
          } else {
            row.setValue(destIdx, value);
          }
        } catch (NumberFormatException e) {
          throw new DirectiveExecutionException(toString(), e);
        }
      } else {
        throw new DirectiveExecutionException(
          String.format("%s : %s was not found or is not of type string. Please check the wrangle configuration.",
                        toString(), col1));
      }
      results.add(row);
    }

    return results;
  }
}
