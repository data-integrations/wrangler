/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.wrangler.steps;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.SkipRowException;
import co.cask.wrangler.api.StepException;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Wrangler step for quantizing a column.
 */
public class Quantization extends AbstractStep {
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
   * @param row Input {@link Row} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return Transformed {@link Row} in which the 'col' value is lower cased.
   * @throws StepException thrown when type of 'col' is not STRING.
   */
  @Override
  public Row execute(Row row, PipelineContext context) throws StepException, SkipRowException {
    Row r = new Row(row);
    int idx = r.find(col1);

    if (idx != -1) {
      try {
        double d = Double.parseDouble((String) r.getValue(idx));
        String value = rangeMap.get(d);
        r.add(col2, value);
      } catch (NumberFormatException e) {
        throw new StepException(toString(), e);
      }
    } else {
      throw new StepException(toString() + " : " +
                                col1 + " was not found or is not of type string. Please check the wrangle configuration."
      );
    }
    return r;
  }
}
