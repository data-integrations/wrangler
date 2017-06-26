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

package co.cask.wrangler.steps.parser;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
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
 * A Json Path Extractor Stage for parsing the {@link Row} provided based on configuration.
 */
@Plugin(type = "udd")
@Name("json-path")
@Usage("json-path <source> <destination> <json-path-expression>")
@Description("Parses JSON elements using a DSL (a JSON path expression).")
public class JsPath extends AbstractDirective {
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
   * Parses a give column in a {@link Row} as a CSV Row.
   *
   * @param rows Input {@link Row} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return New Row containing multiple columns based on CSV parsing.
   * @throws DirectiveExecutionException In case CSV parsing generates more record.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
    List<Row> results = new ArrayList<>();
    for (Row row : rows) {
      Object value = row.getValue(src);
      if (value == null) {
        row.add(dest, null);
        continue;
      }

      if (!(value instanceof String ||
        value instanceof JsonObject ||
        value instanceof JsonArray)) {
        throw new DirectiveExecutionException(
          String.format("%s : Invalid value type '%s' of column '%s'. Should be of type JsonElement, " +
                          "String.", toString(), value.getClass().getName(), src)
        );
      }

      JsonElement element = parser.parse(value).read(path);
      Object val = JsParser.getValue(element);

      // If destination is already present add it, else set the value.
      int pos = row.find(dest);
      if (pos == -1) {
        row.add(dest, val);
      } else {
        row.setValue(pos, val);
      }
      results.add(row);
    }

    return results;
  }
}

