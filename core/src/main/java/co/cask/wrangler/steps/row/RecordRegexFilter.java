/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.wrangler.steps.row;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A Wrangle step for filtering rows that match the pattern specified on the column.
 */
@Usage(
  directive = "filter-row-if-matched",
  usage = "filter-row-if-matched <column> <regex>",
  description = "Filters row if regex is matched."
)
public class RecordRegexFilter extends AbstractStep {
  private final String regex;
  private final String column;
  private Pattern pattern;
  private boolean match;

  public RecordRegexFilter(int lineno, String detail, String column, String regex, boolean match) {
    super(lineno, detail);
    this.regex = regex.trim();
    this.column = column;
    this.match = match;
    if (!regex.equalsIgnoreCase("null")) {
      pattern = Pattern.compile(this.regex);
    } else {
      pattern = null;
    }
  }

  /**
   * Sets the new column names for the {@link Record}.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return A newly transformed {@link Record}.
   * @throws StepException
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    List<Record> results = new ArrayList<>();
    if (pattern == null) {
      return records;
    }
    for (Record record : records) {
      int idx = record.find(column);
      if (idx != -1) {
        Object object = record.getValue(idx);
        if (object == null) {
          continue;
        } else if (object instanceof JSONObject) {
          if (pattern == null && JSONObject.NULL.equals(object)) {
            continue;
          }
        } else if (object instanceof String) {
          String value = (String) record.getValue(idx);
          boolean status = pattern.matcher(value).matches(); // pattern.matcher(value).matches();
          if (!match) {
            status = !status;
          }
          if (status) {
            continue;
          }
        } else {
          throw new StepException(
            String.format("%s : Invalid value type '%s' of column '%s'. Should be of type String.",
                          toString(), object != null ? object.getClass().getName() : "null", column)
          );
        }
        results.add(record);
      }
    }
    return results;
  }
}

