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

import java.util.ArrayList;
import java.util.List;

/**
 * A Wrangler step for splitting a col into two additional columns based on a start and end.
 */
@Usage(
  directive = "indexsplit",
  usage = "indexsplit <source> <start> <end> <destination>",
  description = "[DEPRECATED] Please use 'cut-character' directive."
)
public class IndexSplit extends AbstractDestinationSourceStep {
  // Name of the column to be split
  private String col;

  // Start and end index of the split
  private int start, end;

  // Destination column
  private String dest;

  public IndexSplit(int lineno, String detail, String col, int start, int end, String dest) {
    super(lineno, detail, dest, col);
    this.col = col;
    this.start = start - 1; // Assumes the wrangle configuration starts @ 1
    this.end = end - 1;
    this.dest = dest;
  }

  /**
   * Splits column based on the start and end index.
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
      int idx = record.find(col);

      if (idx != -1) {
        String val = (String) record.getValue(idx);
        if (end > val.length() - 1) {
          end = val.length() - 1;
        }
        if (start < 0) {
          start = 0;
        }
        val = val.substring(start, end);
        record.add(dest, val);
      } else {
        throw new StepException(
          col + " is not of type string in the record. Please check the wrangle configuration."
        );
      }
      results.add(record);
    }
    return results;
  }
}
