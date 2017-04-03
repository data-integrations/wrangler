/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

package co.cask.wrangler.steps.parser;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JsonOrgMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  private JsonProvider provider;

  public JsPath(int lineno, String detail, String src, String dest, String path) {
    super(lineno, detail);
    this.src = src;
    this.dest = dest;
    this.path = path;
    provider = Configuration.defaultConfiguration().jsonProvider();
  }

  /**
   * This will be used for later use, for now we use default provider.
   */
  private static class Config implements Configuration.Defaults {
    private final JsonProvider jsonProvider = new JsonOrgJsonProvider();
    private final MappingProvider mappingProvider = new JsonOrgMappingProvider();

    @Override
    public JsonProvider jsonProvider() {
      return jsonProvider;
    }

    @Override
    public MappingProvider mappingProvider() {
      return mappingProvider;
    }

    @Override
    public Set<Option> options() {
      return EnumSet.noneOf(Option.class);
    }
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
        record.add(dest, null);
        continue;
      }

      // Detect the type of the object, convert it to String before applying JsonPath expression to it.
      if (!(value instanceof String ||
        value instanceof JSONArray ||
        value instanceof net.minidev.json.JSONArray ||
        value instanceof net.minidev.json.JSONObject ||
        value instanceof JSONObject)) {
        throw new StepException(
          String.format("%s : Invalid value type '%s' of column '%s'. Should be of type JSONArray, " +
                          "JSONObject or String.", toString(), value.getClass().getName(), src)
        );
      }
      String v = value.toString();


      // Apply JSON path expression to it.
      Object e = provider.parse(v);
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

