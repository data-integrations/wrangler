/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.steps.transformation;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.annotations.Usage;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Wrangler step for quantizing a column.
 */
@Plugin(type = "udd")
@Name("quantize")
@Usage("quantize <source> <destination> " +
  "<[range1:range2)=value>,[<range1:range2=value>]*")
@Description("Quanitize the range of numbers into label values.")
public class Quantization extends AbstractDirective {
  private static final String RANGE_PATTERN="([+-]?\\d+(?:\\.\\d+)?):([+-]?\\d+(?:\\.\\d+)?)=(.[^,]*)";
  private final RangeMap<Double, String> rangeMap = TreeRangeMap.create();

  private String col1;
  private String col2;

  public Quantization(int lineno, String detail, String col1, String col2, String ranges) {
    super(lineno, detail);
    this.col1 = col1;
    this.col2 = col2;
    Pattern pattern = Pattern.compile(RANGE_PATTERN);
    Matcher matcher = pattern.matcher(ranges);
    while(matcher.find()) {
      double lower = Double.parseDouble(matcher.group(1));
      double upper = Double.parseDouble(matcher.group(2));
      rangeMap.put(Range.closed(lower, upper), matcher.group(3));
    }
  }

  /**
   * Quantizes a column based on the range specified.
   *
   * @param rows Input {@link Row} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return Transformed {@link Row} in which the 'col' value is lower cased.
   * @throws DirectiveExecutionException thrown when type of 'col' is not STRING.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
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
