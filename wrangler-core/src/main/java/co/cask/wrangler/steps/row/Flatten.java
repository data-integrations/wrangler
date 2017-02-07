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

package co.cask.wrangler.steps.row;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive that Flattens a record
 */
public class Flatten extends AbstractStep {
  // Column within the input row that needs to be parsed as Json
  private String[] columns;
  private int[] locations = null;
  private int count = 0;

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

    // Only once find the location of the columns to be flatten within
    // the record. It's assumed that all records to passed to this
    // instance are same.
    if (locations == null) {
      locations = new int[columns.length];
      for (String column : columns) {
        locations[count] = records.get(0).find(column);
        ++count;
      }
    }

    // Iterate through the records.
    for (Record record : records) {

      // For each record we find the maximum number of
      // values in each of the columns specified to be
      // flattened.
      int max = Integer.MIN_VALUE;
      for (int i =0; i < count; ++i) {
        Object value = record.getValue(locations[i]);
        int m = ((JSONArray) value).length();
        if (m > max) {
          max = m;
        }
      }

      // We iterate through the arrays and populate
      // all the columns.
      for(int k = 0; k < max; ++k) {
        Record r = new Record(record);
        for (int i = 0; i < count; ++i) {
          Object value = record.getValue(locations[i]);
          // Record might not have the column itself.
          // So, we add 'null' to that column.
          if (value == null) {
            r.add(columns[i], null);
          } else {
            if (value instanceof JSONArray) {
              if (((JSONArray) value).get(k) == null) {
                r.add(columns[i], null);
              } else {
                r.setValue(locations[i], ((JSONArray) value).get(k));
              }
            }
          }
        }

        results.add(r);
      }
    }
    return results;
  }

}
