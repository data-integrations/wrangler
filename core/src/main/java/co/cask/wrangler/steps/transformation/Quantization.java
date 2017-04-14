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

import co.cask.wrangler.api.AbstractDestinationSourceStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
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
@Usage(
  directive = "quantize",
  usage = "quantize <source> <destination> " +
  "<[range1:range2)=value>,[<range1:range2=value>]*",
  description = "Quanitize the range of numbers into label values."
)
public class Quantization extends AbstractDestinationSourceStep {
  private static final String RANGE_PATTERN="([+-]?\\d+(?:\\.\\d+)?):([+-]?\\d+(?:\\.\\d+)?)=(.[^,]*)";
  private final RangeMap<Double, String> rangeMap = TreeRangeMap.create();

  private String source;
  private String destination;

  public Quantization(int lineno, String detail, String source, String destination, String ranges) {
    super(lineno, detail, destination, source);
    this.source = source;
    this.destination = destination;
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
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return Transformed {@link Record} in which the 'col' value is lower cased.
   * @throws StepException thrown when type of 'col' is not STRING.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    List<Record> results = new ArrayList<>();
    for (Record record : records) {
      int idx = record.find(source);

      if (idx != -1) {
        try {
          Object object = record.getValue(idx);
          Double d;
          if (object instanceof String) {
            d = Double.parseDouble((String) object);
          } else if (object instanceof Double) {
            d = (Double) object;
          } else if (object instanceof Float) {
            d = ((Float) object).doubleValue();
          } else {
            throw new StepException(
              String.format("%s : Invalid type '%s' of column '%s'. Should be of type String, Float or Double.",
                            toString(), object != null ? object.getClass().getName() : "null", source)
            );
          }
          String value = rangeMap.get(d);
          int destIdx = record.find(destination);
          if (destIdx == -1) {
            record.add(destination, value);
          } else {
            record.setValue(destIdx, value);
          }
        } catch (NumberFormatException e) {
          throw new StepException(toString(), e);
        }
      } else {
        throw new StepException(
          String.format("%s : %s was not found or is not of type string. Please check the wrangle configuration.",
                        toString(), source));
      }
      results.add(record);
    }

    return results;
  }
}
