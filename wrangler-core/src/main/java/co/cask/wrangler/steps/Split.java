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
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.List;

/**
 * A Wrangler step for splitting a col into two additional columns based on a delimiter.
 */
public class Split extends AbstractStep {
  // Name of the column to be split
  private String col;

  private String delimiter;

  // Destination column names
  private String firstColumnName, secondColumnName;

  public Split(int lineno, String detail, String col,
               String delimiter, String firstColumnName, String secondColumnName) {
    super(lineno, detail);
    this.col = col;
    this.delimiter = delimiter;
    this.firstColumnName = firstColumnName;
    this.secondColumnName = secondColumnName;
  }

  /**
   * Splits column based on the delimiter into two columns.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return Transformed {@link Record} which contains two additional columns based on the split
   * @throws StepException thrown when type of 'col' is not STRING.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    List<Record> results = new ArrayList<>();
    for (Record record : records) {
      int idx = record.find(col);
      if (idx != -1) {
        String val = (String) record.getValue(idx);
        if (val != null) {
          String[] parts = val.split(delimiter, 2);
          if (Strings.isNullOrEmpty(parts[0])) {
            record.add(firstColumnName, parts[1]);
            record.add(secondColumnName, null);
          } else {
            record.add(firstColumnName, parts[0]);
            record.add(secondColumnName, parts[1]);
          }
        } else {
          record.add(firstColumnName, null);
          record.add(secondColumnName, null);
        }
      } else {
        throw new StepException(
          col + " is not of type string. Please check the wrangle configuration."
        );
      }
      results.add(record);
    }
    return results;
  }
}
