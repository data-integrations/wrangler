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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.pipeline.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import co.cask.wrangler.steps.parser.JsParser;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive that Flattens a record
 */
@Plugin(type = "udd")
@Name("flatten")
@Usage("flatten <column>[,<column>*]")
@Description("Separates array elements of one or more columns into indvidual records, copying the other columns.")
public class Flatten extends AbstractStep {
  // Column within the input row that needs to be parsed as Json
  private String[] columns;
  private int[] locations;
  private int count = 0;

  public Flatten(int lineno, String detail, String[] columns) {
    super(lineno, detail);
    this.columns = columns;
    this.locations = new int[columns.length];
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

    // Iterate through the records.
    for (Record record : records) {
      count = 0;
      // Only once find the location of the columns to be flatten within
      // the record. It's assumed that all records to passed to this
      // instance are same.
      for (String column : columns) {
        locations[count] = record.find(column);
        ++count;
      }
      // For each record we find the maximum number of
      // values in each of the columns specified to be
      // flattened.
      int max = Integer.MIN_VALUE;
      for (int i = 0; i < count; ++i) {
        if (locations[i] != -1) {
          Object value = record.getValue(locations[i]);
          int m = -1;
          if (value instanceof JsonArray) {
            m = ((JsonArray) value).size();
          } else if (value instanceof List){
            m = ((List) value).size();
          } else {
            m = 1;
          }
          if (m > max) {
            max = m;
          }
        }
      }

      // We iterate through the arrays and populate all the columns.
      for(int k = 0; k < max; ++k) {
        Record r = new Record(record);
        for (int i = 0; i < count; ++i) {
          if (locations[i] != -1) {
            Object value = record.getValue(locations[i]);
            if (value == null) {
              r.add(columns[i], null);
            } else {
              Object v = null;
              if (value instanceof JsonArray) {
                JsonArray array = (JsonArray) value;
                if (k < array.size()) {
                  v = array.get(k);
                }
              } else if (value instanceof List) {
                List<Object> array = (List) value;
                if (k < array.size()) {
                  v = array.get(k);
                }
              } else {
                v = value;
              }
              if (v == null) {
                r.addOrSet(columns[i], null);
              } else {
                if (v instanceof JsonElement) {
                  r.setValue(locations[i], JsParser.getValue((JsonElement)v));
                } else {
                  r.setValue(locations[i], v);
                }
              }
            }
          } else {
            r.addOrSet(columns[i], null);
          }
        }
        results.add(r);
      }
    }
    return results;
  }

}
