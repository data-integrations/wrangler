/*
 *  Copyright © 2017 Cask Data, Inc.
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

package co.cask.directives.writer;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Optional;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.UDD;
import co.cask.wrangler.api.annotations.Usage;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.ColumnNameList;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import java.util.List;

/**
 * A directive for writing selected columns as Json Objects.
 */
@Plugin(type = UDD.Type)
@Name("write-as-json-object")
@Usage("write-as-json-object <dest-column> [<src-column>[,<src-column>]")
@Description("Creates a JSON object based on source columns specified. JSON object is written into dest-column.")
public class WriteAsJsonObject implements UDD {
  public static final String NAME = "write-as-json-object";
  private String column;
  private List<String> columns;
  private Gson gson;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("col", TokenType.COLUMN_NAME_LIST, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.columns = ((ColumnNameList) args.value("col")).value();
    this.gson = new Gson();
  }

  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      JsonObject object = new JsonObject();
      for (String col : columns) {
        Object value = row.getValue(col);
        if (value instanceof Integer) {
          object.addProperty(col, (Integer) value);
        } else if (value instanceof Long) {
          object.addProperty(col, (Long) value);
        } else if (value instanceof Number) {
          object.addProperty(col, (Number) value);
        } else if (value instanceof Float) {
          object.addProperty(col, (Float) value);
        } else if (value instanceof Double) {
          object.addProperty(col, (Double) value);
        } else if (value instanceof Short) {
          object.addProperty(col, (Short) value);
        } else if (value instanceof Character) {
          object.addProperty(col, Character.toString((Character) value));
        } else if (value instanceof String) {
          object.addProperty(col, (String) value);
        } else if (value instanceof JsonElement) {
          object.add(col, (JsonElement) value);
        } else if (value instanceof JsonNull) {
          object.add(col, (JsonNull) value);
        }
      }
      row.addOrSet(column, object);
    }
    return rows;
  }
}
