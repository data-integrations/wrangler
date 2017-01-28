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
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Fixed length Parser Stage for parsing the {@link Record} provided based on configuration.
 */
public final class FixedLengthParser extends AbstractStep {
  private final RangeSet<Integer> ranges;
  private final String col;

  public FixedLengthParser(int lineno, String detail, String col, String rangeText) {
    super(lineno, detail);
    this.col = col;
    ranges = getRanges(rangeText);
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records     Input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled {@link Record}.
   * @throws StepException In case of any issue this exception is thrown.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    List<Record> results = new ArrayList<>();
    for (Record record : records) {
      int idx = record.find(col);
      if (idx != -1) {
        Object v = record.getValue(idx);
        if (v instanceof String) {
          String value = (String) v;
          Set<Range<Integer>> ls = ranges.asRanges();
          int i = 1;
          for (Range<Integer> l : ls) {
            int start = l.lowerEndpoint() - 1;
            int end = l.upperEndpoint();
            if (end - start == 0 ) {
              end = end + 1;
            }
            if (end < value.length() + 1) {
              record.add(String.format("%s_col%d", col, i), value.substring(start, end));
              i++;
            }
          }
        } else {
          throw new StepException(
            String.format("%s : Invalid type of column '%s'. Should be of type String.", toString(),
                          col)
          );
        }
      }
      results.add(record);
    }
    return results;
  }

  private RangeSet<Integer> getRanges(String text) {
    RangeSet<Integer> ranges = TreeRangeSet.create();
    Pattern re_next_val = Pattern.compile(
      "# extract next integers/integer range value.    \n" +
        "([0-9]+)      # $1: 1st integer (Base).         \n" +
        "(?:           # Range for value (optional).     \n" +
        "  -           # Dash separates range integer.   \n" +
        "  ([0-9]+)    # $2: 2nd integer (Range)         \n" +
        ")?            # Range for value (optional). \n" +
        "(?:,|$)       # End on comma or string end.",
      Pattern.COMMENTS);
    Matcher m = re_next_val.matcher(text);
    while (m.find()) {
      int start = Integer.parseInt(m.group(1));
      int end = start;
      if (m.group(2) != null) {
        end = Integer.parseInt(m.group(2));
      }
      ranges.add(Range.closed(start, end));
    }
    return ranges;
  }

}