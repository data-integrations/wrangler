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

package co.cask.wrangler.steps.row;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.ErrorRecordException;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.util.ArrayList;
import java.util.List;

/**
 * A step for parsing a string into record using the record delimiter.
 */
@Usage(
  directive = "set-record-delim",
  usage = "set-record-delim <column> <delimiter> [<limit>]",
  description = "Sets the record delimiter."
)
public class SetRecordDelimiter extends AbstractStep {
  private final String column;
  private final String delimiter;
  private final int limit;

  public SetRecordDelimiter(int lineno, String detail, String column, String delimiter, int limit) {
    super(lineno, detail);
    this.column = column;
    this.delimiter = delimiter;
    this.limit = limit;
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records List of input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled List of {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context)
    throws StepException, ErrorRecordException {
    List<Record> results = new ArrayList<>();
    for (Record record : records) {
      int idx = record.find(column);
      if (idx == -1) {
        continue;
      }

      Object object = record.getValue(idx);
      if (object instanceof String) {
        String body = (String) object;
        String[] lines = body.split(delimiter);
        int i = 0;
        for (String line : lines) {
          if (i > limit) {
            break;
          }
          results.add(new Record(column, line));
          i++;
        }
      }
    }
    return results;
  }
}
