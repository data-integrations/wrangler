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
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A Json Parser Stage for parsing the {@link Record} provided based on configuration.
 */
public class JsonParser extends AbstractStep {
  // Column within the input row that needs to be parsed as Json
  private String col;
  private boolean delete;

  public JsonParser(int lineno, String detail, String col, boolean delete) {
    super(lineno, detail);
    this.col = col;
    this.delete = delete;
  }

  /**
   * Parses a give column in a {@link Record} as a CSV Record.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return New Row containing multiple columns based on CSV parsing.
   * @throws StepException In case CSV parsing generates more record.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    List<Record> results = new ArrayList<>();

    // Iterate through all the records.
    for (Record record : records) {

      int idx = record.find(col);
      if (idx != -1) {
        Object value = record.getValue(idx);

        JSONObject object = null;
        try {
          if (value instanceof String) {
            object = new JSONObject((String) value);
          } else if (value instanceof JSONObject) {
            object = (JSONObject) value;
          } else {
            throw new StepException(
              String.format("%s : Invalid type '%s' of column '%s'. " +
                              "Should be of type JSONObject or String. Use <xml|json>-path to parser data.", toString(),
                            col, value.getClass().getName())
            );
          }
          // Iterate through keys.
          Iterator<String> keysItr = object.keys();
          while(keysItr.hasNext()) {
            String key = keysItr.next();
            Object v = object.get(key);
            record.add(String.format("%s.%s", col, key), v);
          }

          // Delete the original column.
          if (delete) {
            record.remove(idx);
          }
          results.add(record);
        } catch (JSONException e) {
          throw new StepException(toString() + " : " + e.getMessage());
        }
      } else {
        throw new StepException(toString() + " : Did not find '" + col + "' in the record.");
      }
    }
    return results;
  }

}
