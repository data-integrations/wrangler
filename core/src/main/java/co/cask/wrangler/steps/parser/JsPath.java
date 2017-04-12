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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;

import java.util.ArrayList;
import java.util.List;

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
  private ParseContext parser;

  public static final Configuration GSON_CONFIGURATION = Configuration
    .builder()
    .mappingProvider(new GsonMappingProvider())
    .jsonProvider(new GsonJsonProvider())
    .build();

  public JsPath(int lineno, String detail, String src, String dest, String path) {
    super(lineno, detail);
    this.src = src;
    this.dest = dest;
    this.path = path;
    this.parser = JsonPath.using(GSON_CONFIGURATION);
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

      if (!(value instanceof String ||
        value instanceof JsonObject ||
        value instanceof JsonArray)) {
        throw new StepException(
          String.format("%s : Invalid value type '%s' of column '%s'. Should be of type JsonElement, " +
                          "String.", toString(), value.getClass().getName(), src)
        );
      }

      JsonElement element = parser.parse(value).read(path);
      Object val = JsParser.getValue(element);

      // If destination is already present add it, else set the value.
      int pos = record.find(dest);
      if (pos == -1) {
        record.add(dest, val);
      } else {
        record.setValue(pos, val);
      }
      results.add(record);
    }

    return results;
  }
}

