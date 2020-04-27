/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.directives.parser;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * A Json Path Extractor Stage for parsing the {@link Row} provided based on configuration.
 */
@Plugin(type = Directive.TYPE)
@Name("json-path")
@Categories(categories = { "parser", "json"})
@Description("Parses JSON elements using a DSL (a JSON path expression).")
public class JsPath implements Directive, Lineage {
  public static final String NAME = "json-path";
  private String src;
  private String dest;
  private String path;
  private ParseContext parser;

  public static final Configuration GSON_CONFIGURATION = Configuration
    .builder()
    .mappingProvider(new GsonMappingProvider())
    .jsonProvider(new GsonJsonProvider())
    .build();

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("source", TokenType.COLUMN_NAME);
    builder.define("destination", TokenType.COLUMN_NAME);
    builder.define("json-path", TokenType.TEXT);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.src = ((ColumnName) args.value("source")).value();
    this.dest = ((ColumnName) args.value("destination")).value();
    this.path = ((Text) args.value("json-path")).value();
    this.parser = JsonPath.using(GSON_CONFIGURATION);
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
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
          NAME, String.format("Column '%s' is of invalid type '%s'. It should be of type 'String' " +
                                "or 'JsonObject' or 'JsonArray'.", src, value.getClass().getSimpleName())
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

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Extracted value from column '%s' represented as Json to destination column '%s' using path '%s'",
                src, dest, path)
      .conditional(src, dest)
      .build();
  }
}

