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

package co.cask.wrangler.steps.parser;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A Json Parser Stage for parsing the {@link Record} provided based on configuration.
 */
@Usage(
  directive = "parse-as-json",
  usage = "parse-as-json <column> <delete-column>",
  description = "Parses a column as JSON."
)
public class JsonParser extends AbstractStep {
  // Column within the input row that needs to be parsed as Json
  private String col;
  private int maxDepth;

  public JsonParser(int lineno, String detail, String col, int maxDepth) {
    super(lineno, detail);
    this.col = col;
    this.maxDepth = maxDepth;
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
      boolean delete = false;
      int idx = record.find(col);

      // If the input column exists in the record, proceed further.
      if (idx != -1) {
        Object value = record.getValue(idx);

        if (value == null) {
          continue;
        }

        JSONObject object = null;
        JSONArray list = null;

        try {
          // If the input is a string, then it can either be an Array or Object.
          if (value instanceof String) {
            Object json = new JSONTokener((String) value).nextValue();
            if (json instanceof JSONObject) {
              object = (JSONObject) json;
            } else if (json instanceof JSONArray) { // Delete the source column when it's an array.
              list = (JSONArray) json;
              delete = true;
            }
          } else if (value instanceof JSONObject) {
            object = (JSONObject) value;
          } else if (value instanceof JSONArray) { // Delete the source column when it's an array.
            list = (JSONArray) value;
            delete = true;
          } else {
            throw new StepException(
              String.format("%s : Invalid type '%s' of column '%s'. " +
                              "Should be of type JSONObject or String. Use paths to further parse data.",
                            toString(), object != null ? object.getClass().getName() : "null", col)
            );
          }

          // Delete the original column.
          if (delete) {
            record.remove(idx);
          }

          if (object != null) { // Iterate through keys.
            flattenJson(object, col, 1, maxDepth, record);
            results.add(record);
          }

          // It's an array of objects.
          if(list != null) {
            int index = 0;
            while(index < list.length()) {
              Object type = list.get(index);
              if (type instanceof JSONObject) {
                Record objectRecord = new Record(record);
                flattenJson((JSONObject) type, col, 1, maxDepth, objectRecord);
                results.add(objectRecord);
              } else {
                break;
              }
              index++;
            }

            index = 0;
            while(index < list.length()) {
              Object type = list.get(index);
              if(!(type instanceof JSONArray) && !(type instanceof JSONObject)) {
                record.add(String.format("%s_%d", col, index + 1), type);
              } else {
                break;
              }
              index++;
            }
            if (index == list.length()) {
              results.add(record);
            }
          }
        } catch (JSONException e) {
          throw new StepException(toString() + " : " + e.getMessage());
        }
      } else {
        throw new StepException(toString() + " : Did not find '" + col + "' in the record.");
      }
    }
    return results;
  }

  /**
   * Recursively flattens JSON until the 'maxDepth' is reached.
   *
   * @param root of the JSONObject
   * @param field name to be used to be stored in the record.
   * @param depth current depth into JSON structure.
   * @param maxDepth maximum depth to reach
   * @param record to which the flatten fields need to be added.
   */
  public static void flattenJson(JSONObject root, String field, int depth, int maxDepth, Record record) {
    if (depth > maxDepth) {
      record.addOrSet(String.format("%s", field), root);
      return;
    }
    Iterator<String> keysItr = root.keys();
    while(keysItr.hasNext()) {
      String key = keysItr.next();
      Object value = root.get(key);
      if (value instanceof JSONObject) {
        flattenJson((JSONObject)value, String.format("%s_%s", field, key), depth++, maxDepth, record);
      } else {
        record.addOrSet(String.format("%s_%s", field, key), value);
      }
    }
  }

}
