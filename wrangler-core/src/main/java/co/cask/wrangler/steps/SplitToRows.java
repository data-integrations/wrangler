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

package co.cask.wrangler.steps;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.util.ArrayList;
import java.util.List;

/**
 * A Split on Stage for splitting the string into multiple {@link Record}s.
 */
@Usage(directive = "split-to-rows", usage = "split-to-rows <column> <separator>")
public class SplitToRows extends AbstractStep {
  // Column on which to apply mask.
  private final String column;

  // Type of mask.
  private final String regex;

  public SplitToRows(int lineno, String detail, String column, String regex) {
    super(lineno, detail);
    this.column = column;
    this.regex = regex;
  }

  /**
   * Splits a record into multiple records based on separator.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return A newly transformed {@link Record} with masked column.
   * @throws StepException thrown when there is issue with masking
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    List<Record> results = new ArrayList<>();

    for (Record record : records) {
      int idx = record.find(column);
      if (idx != -1) {
        Object object = record.getValue(idx);
        if (object != null && object instanceof String) {
          String[] lines = ((String) object).split(regex);
          for (String line : lines) {
            Record r = new Record(record);
            r.setValue(idx, line);
            results.add(r);
          }
        } else {
          throw new StepException(
            String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.", toString(),
                          object != null ? object.getClass().getName() : "null", column)
          );

        }
      }
    }
    return results;
  }
}

