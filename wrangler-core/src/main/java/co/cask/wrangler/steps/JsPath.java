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

package co.cask.wrangler.steps;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A Json Path Extractor Stage for parsing the {@link Record} provided based on configuration.
 */
@Usage(
  directive = "json-path",
  usage = "json-path <source> <destination> <json path>",
  description = "Parses JSON elements using JSON paths."
)
public class JsPath extends AbstractStep {
  private String src;
  private String dest;
  private String path;

  public JsPath(int lineno, String detail, String src, String dest, String path) {
    super(lineno, detail);
    this.src = src;
    this.dest = dest;
    this.path = path;
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
    for (Record record : records) {
      Object value = record.getValue(src);
      if (value == null) {
        throw new StepException(toString() + " : Did not find field '" + src + "' in the record.");
      }

      // Detect the type of the object, convert it to String before apply JsonPath
      // expression to it.
      String v = null;
      if (value instanceof String) {
        v = (String) value;
      } else if (value instanceof JSONArray) {
        v = ((JSONArray) value).toString();
      } else if (value instanceof net.minidev.json.JSONArray) {
        v = ((net.minidev.json.JSONArray) value).toString();
      } else if (value instanceof net.minidev.json.JSONObject) {
        v = ((net.minidev.json.JSONObject) value).toString();
      } else if (value instanceof JSONObject) {
        v = ((JSONObject) value).toString();
      } else {
        throw new StepException(
          String.format("%s : Invalid value type '%s' of column '%s'. Should be of type JSONArray, " +
                          "JSONObject or String.", toString(), value.getClass().getName(), src)
        );
      }

      // Apply JSON path expression to it.
      Object e = Configuration.defaultConfiguration().jsonProvider().parse(v);
      Object object = JsonPath.read(e, path);

      // Check if the objects are arrays, if so convert it to List<Object>
      if (object instanceof net.minidev.json.JSONArray) {
        JSONArray objects = new JSONArray();
        for(int i = 0; i < ((net.minidev.json.JSONArray) object).size(); ++i) {
          objects.put(((net.minidev.json.JSONArray) object).get(i));
        }
        object = objects;
      }

      if (object instanceof Map) {
        JSONObject objects = new JSONObject();
        for (Map.Entry<String, String> entry : ((Map<String, String>)object).entrySet()) {
          objects.put(entry.getKey(), entry.getValue());
        }
        object = objects;
      }

      // If destination is already present add it, else set the value.
      int pos = record.find(dest);
      if (pos == -1) {
        record.add(dest, object);
      } else {
        record.setValue(pos, object);
      }
      results.add(record);
    }

    return results;
  }
}

