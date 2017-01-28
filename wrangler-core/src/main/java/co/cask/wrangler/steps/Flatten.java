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

import java.util.ArrayList;
import java.util.List;

/**
 * Flattens a record based on the columns specified.
 */
public class Flatten extends AbstractStep {
  // Column within the input row that needs to be parsed as Json
  private String[] columns;

  public Flatten(int lineno, String detail, String[] columns) {
    super(lineno, detail);
    this.columns = columns;
  }

  /**
   * Flattens a record based on the columns specified to be flattened.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return New Row containing multiple columns based on CSV parsing.
   * @throws StepException In case CSV parsing generates more record.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    List<Record> results = new ArrayList<>();

    int[] locations = new int[columns.length];
    int count = 0;
    for (String column : columns) {
      locations[count] = records.get(0).find(column);
      ++count;
    }


    for (Record record : records) {
      int max = Integer.MIN_VALUE;
      for (int i =0; i < count; ++i) {
        Object value = record.getValue(locations[i]);
        int m = ((List) value).size();
        if (m > max) {
          max = m;
        }
      }

      for(int k = 0; k < max; ++k) {
        Record r = new Record(record);
        for (int i = 0; i < count; ++i) {
          Object value = record.getValue(locations[i]);
          if (value instanceof List) {
            r.setValue(locations[i], ((List) value).get(k));
          }
        }
        results.add(r);
      }
    }
    return results;
  }

}
